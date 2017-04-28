import os
import MySQLdb
import settings
import urllib2
import json
import time
import string
import argparse
import datetime
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser(description='ETL for getting drupal 7 post and user data and web site pages into JSON lines files')
parser.add_argument(
    '--limit',
    help='INT: limit initial node query',
    dest='limit',
    type=int,
    required=False
)

parser.add_argument(
    '--clean',
    help=(
        'Flag to empty the output directory to clean up from previous runs.'
    ),
    action='store_true',
    dest='clean',
    required=False,
)

parser.add_argument(
    '--dry-run',
    help=(
        'Flag to not save any data, just return printed info about what would happen'
    ),
    action='store_true',
    dest='dry_run',
)

args = parser.parse_args()

kwargs = {}
if settings.mysql_pw:
    kwargs['passwd'] = settings.mysql_pw
if settings.mysql_user:
    kwargs['user'] = settings.mysql_user
if settings.mysql_host:
    kwargs['host'] = settings.mysql_host
if settings.mysql_db:
    kwargs['db'] = settings.mysql_db
if settings.mysql_port:
    kwargs['port'] = settings.mysql_port

db = MySQLdb.connect(**kwargs)


def get_nodes_to_export_from_db():
    # this query gets back the identifying information for the posts we want
    # as well as the node and full path URLs that we can use to get the current
    # actual HTML page
    node_query = (
        "select n.nid, n.changed, n.type as content_type, u.alias, u.source "
        "from node n join url_alias u on u.source = CONCAT('node/', n.nid) "
        "where n.type ='post' and n.status = 1 "
        "and u.pid=(select pid from url_alias where source = u.source order by pid desc limit 1) "
        "order by n.changed DESC "
    )
    if args.limit:
        node_query = node_query + 'limit %s' % args.limit
    node_cursor = db.cursor()
    node_cursor.execute(node_query)
    nodes = []
    for nid, changed, content_type, alias, source in node_cursor:
        node_data = {
            'nid': nid,
            'changed': changed,
            'content_type': content_type,
            'node_url_path': source,
            'url_path': alias,
        }
        nodes.append(node_data)
    return nodes


def printable(string_in):
    """ Return a string with any non-printable characters removed
    """
    filtered_chars = []
    printable_chars = set(string.printable)
    for char in string_in:
        if char in printable_chars:
            filtered_chars.append(char)
        else:
            filtered_chars.append(' ')
    return ''.join(filtered_chars)


def get_page_title(page):
    return page.head.title.text


def get_canonical_url(page):
    return page.head.find('link', rel='canonical')['href']


def get_meta_description(page):
    for m in page.head.find_all('meta'):
        if m.get('name') == 'description':
            return m['content']
    return None


def get_story_title(page):
    return page.body.find('div', property='dc:title').h3.a.text


def get_user_db_data(username):
    # derive the UID from the source URL for the profile:
    user_url_alias_query = "select substring(source, 6) as uid from url_alias where alias = 'users/%s'" % username
    user_url_alias_cursor = db.cursor()
    user_url_alias_cursor.execute(user_url_alias_query)
    for uid in user_url_alias_cursor:
        uid = uid

    user_query = "select uid, mail from users where uid = %s" % uid
    user_cursor = db.cursor()
    user_cursor.execute(user_query)
    for uid, mail in user_cursor:
        return uid, mail


def get_pub_and_author_info(page):
    pub_date_div = page.body.find('div', {'class': 'field-name-post-date-author-name'})
    author_name = pub_date_div.p.a.text
    pub_date = pub_date_div.p.text.replace(author_name, '').strip()
    author_link = pub_date_div.p.a['href']
    author_username = author_link.split('/')[-1]
    return author_name, author_link, author_username, pub_date


def get_story_body(page):
    body_div = page.body.find('div', {'class': 'field-name-body'})
    body_content_div = body_div.find('div', {'property': 'content:encoded'})
    return printable(body_content_div.decode_contents(formatter="html"))


def get_author_div_text(page, target_class):
    """ Mostly all of the fields in the author profile are nested in the same
        little template pattern. Here's a generic function for pulling out the
        text of a given field class.
    """
    target_div = page.body.find('div', {'class': target_class})
    if target_div:
        target_content_div = target_div.find('div', {'class': 'field-item even'})
        result = target_content_div.text.strip()
        return printable(result)
    return None


def get_author_website_url(page):
    div = page.body.find('div', {'class': 'field-name-field-user-website'})
    if div:
        link = div.find('a')
        return link['href']
    return None


def get_author_headshot_url(page):
    image_div = page.body.find('div', {'class': 'field-name-ds-user-picture'})
    if image_div:
        image_div = image_div.find('img')
        return image_div['src']
    return None


def get_author_social_urls(page):
    social_network_link_ids = [
        'facebook',
        'twitter',
        'linkedin',
        'google',
    ]
    social_network_links = {}
    for link_id in social_network_link_ids:
        link_div = page.body.find('div', {'class': 'field-name-field-user-%s-url' % link_id})
        if link_div:
            link_content_div = link_div.find('div', {'class': 'field-item even'})
            social_network_links[link_id] = link_content_div.a['href']
        else:
            social_network_links[link_id] = None
    return social_network_links


def get_page(url):
    try:
        opener = urllib2.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        response = opener.open(url)
        page_content = response.read()
        page = BeautifulSoup(page_content, "html.parser")
        return page, None
    except Exception, e:
        return None, str(e)


def write_html_content_to_output(nodes_to_export):
    file_time = time.strftime("%Y-%m-%d_%H:%M:%S")
    outfile_story = open('output/%s-story.jl' % file_time, 'w+')
    outfile_user = open('output/%s-user.jl' % file_time, 'w+')
    error_file = None
    author_links = []
    story_success_count = 0
    user_success_count = 0
    error_count = 0
    for node in nodes_to_export:
        full_url = settings.site_url + '/' + node['url_path']
        page, error = get_page(full_url)
        if page:
            node['title'] = get_page_title(page)
            node['canonical_url'] = get_canonical_url(page)
            node['meta_description'] = get_meta_description(page)
            node['story_title'] = get_story_title(page)
            node['topic'] = node['url_path'].split('/')[0]
            (
                node['author_name'],
                node['author_link'],
                node['author_username'],
                node['pub_date']
            ) = get_pub_and_author_info(page)
            node['story_body'] = get_story_body(page)
            json_string = json.dumps(node)
            outfile_story.write(json_string + '\n')
            if node['author_link'] not in author_links:
                author_links.append(node['author_link'])

                uid, email = get_user_db_data(node['author_username'])
                user_page_data = {
                    'uid': uid,
                    'email': email,
                    'profile_url': node['author_link'],
                    'username': node['author_username']
                }
                user_page, user_page_error = get_page(node['author_link'])
                if user_page:
                    user_page_data['content_type'] = 'user'
                    user_page_data['bio'] = get_author_div_text(user_page, 'field-name-field-user-biography')
                    user_page_data['fullname'] = get_author_div_text(user_page, 'field-name-user-full-name')
                    user_page_data['company_name'] = get_author_div_text(user_page, 'field-name-field-user-company-name')
                    user_page_data['job_title'] = get_author_div_text(user_page, 'field-name-field-user-job-title')
                    user_page_data['headshot_url'] = get_author_headshot_url(user_page)
                    user_page_data['website'] = get_author_website_url(user_page)

                    social_link_urls = get_author_social_urls(user_page)
                    user_page_data['facebook_url'] = social_link_urls['facebook']
                    user_page_data['twitter_url'] = social_link_urls['twitter']
                    user_page_data['linkedin_url'] = social_link_urls['linkedin']
                    user_page_data['google_url'] = social_link_urls['google']
                    user_success_count += 1
                else:
                    if not error_file:
                        error_file = open('output/%s-error.jl' % file_time, 'w+')
                    error_data = {
                        'type': 'user page',
                        'url': node['author_link'],
                        'error': user_page_error,
                    }
                    error_data_string = json.dumps(error_data)
                    error_file.write(error_data_string + '\n')
                    error_count += 1

                # we want the user_page_data written outside of the if user_page
                # block because a user needs to be created to link the story
                # object to even if we can't get their profile right now.
                json_string = json.dumps(user_page_data)
                outfile_user.write(json_string + '\n')

            story_success_count += 1
        else:
            if not error_file:
                error_file = open('output/%s-error.jl' % file_time, 'w+')
            error_data = {
                'type': 'story page',
                'url': full_url,
                'error': error,
            }
            error_data_string = json.dumps(error_data)
            error_file.write(error_data_string + '\n')
            error_count += 1

    outfile_story.close()
    outfile_user.close()
    if error_file:
        error_file.close()

    return story_success_count, user_success_count, error_count


def main():
    print "GIVE MY CREATION LIFE"
    starttime = datetime.datetime.now()
    if args.clean:
        filelist = [f for f in os.listdir("output/") if f.endswith(".jl")]
        print "cleaning %d files from output/" % len(filelist)
        for f in filelist:
            os.remove("output/%s" % f)

    nodes_to_export = get_nodes_to_export_from_db()
    if len(nodes_to_export) > 0:
        print "Found %d stories to export" % len(nodes_to_export)
        if args.dry_run:
            return
        story_success_count, user_success_count, error_count = write_html_content_to_output(nodes_to_export)
        print "Parsed %d stories" % story_success_count
        print "Parsed %d users" % user_success_count
        print "Had %d HTTP errors" % error_count
    else:
        print "Didn't find anything to export"
    endtime = datetime.datetime.now()
    total_time = endtime - starttime
    print "total time: %s" % total_time
if __name__ == "__main__":
    main()
