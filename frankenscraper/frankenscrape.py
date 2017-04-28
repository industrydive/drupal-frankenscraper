import MySQLdb
import settings
import sys
import urllib2
import json
import time
from bs4 import BeautifulSoup

TESTING = sys.argv[0].endswith('nosetests')

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
    node_query = (
        "select nid, changed, type as content_type from node where type in "
        "('post', 'podcast', 'infographic') "
        "and status = 1 order by changed DESC limit 5"
    )
    node_cursor = db.cursor()
    node_cursor.execute(node_query)
    nodes = []
    for nid, changed, content_type in node_cursor:
        node_data = {'nid': nid, 'changed': changed, 'content_type': content_type}

        url_alias_query = "select source, alias from url_alias where source = 'node/%s'" % nid
        url_alias_cursor = db.cursor()
        url_alias_cursor.execute(url_alias_query)

        for source, alias in url_alias_cursor:
            node_data['node_url_path'] = source
            node_data['url_path'] = alias
        nodes.append(node_data)
    return nodes


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
    return body_content_div.decode_contents(formatter="html")


def get_author_div_text(page, target_class):
    """ Mostly all of the fields in the author profile are nested in the same
        little template pattern. Here's a generic function for pulling out the
        text of a given field class.
    """
    target_div = page.body.find('div', {'class': target_class})
    if target_div:
        target_content_div = target_div.find('div', {'class': 'field-item even'})
        return target_content_div.text.strip()
    return None


def get_author_bio(page):
    return get_author_div_text(page, 'field-name-field-user-biography')


def get_author_fullname(page):
    return get_author_div_text(page, 'field-name-user-full-name')


def get_author_company_name(page):
    return get_author_div_text(page, 'field-name-field-user-company-name')


def get_author_job_title(page):
    return get_author_div_text(page, 'field-name-field-user-job-title')


def get_author_website_url(page):
    div = page.body.find('div', {'class': 'field-name-field-user-website'})
    if div:
        link = div.find('a')
        return link['href']
    return None


def get_author_headshot_url(page):
    image_div = page.body.find('div', {'class': 'field-name-ds-user-picture'})
    image_div = image_div.find('img')
    return image_div['src']


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
        return page
    except Exception:
        return None


def write_html_content_to_output(nodes_to_export):
    file_time = time.strftime("%Y-%m-%d_%H:%M:%S")
    outfile = open('output/%s.jl' % file_time, 'w+')
    author_links = []
    for node in nodes_to_export:
        full_url = settings.site_url + '/' + node['url_path']
        page = get_page(full_url)
        if page:
            node['title'] = get_page_title(page)
            node['canonical_url'] = get_canonical_url(page)
            node['meta_description'] = get_meta_description(page)
            node['story_title'] = get_story_title(page)
            (
                node['author_name'],
                node['author_link'],
                node['author_username'],
                node['pub_date']
            ) = get_pub_and_author_info(page)
            node['story_body'] = get_story_body(page)
            json_string = json.dumps(node)
            outfile.write(json_string + '\n')
            if node['author_link'] not in author_links:
                author_links.append(node['author_link'])

                uid, email = get_user_db_data(node['author_username'])
                page_data = {
                    'uid': uid,
                    'email': email,
                    'profile_url': node['author_link'],
                    'username': node['author_username']
                }
                page = get_page(node['author_link'])
                if page:
                    page_data['content_type'] = 'user'
                    page_data['bio'] = get_author_bio(page)
                    page_data['fullname'] = get_author_fullname(page)
                    page_data['company_name'] = get_author_company_name(page)
                    page_data['job_title'] = get_author_job_title(page)
                    page_data['headshot_url'] = get_author_headshot_url(page)
                    social_link_urls = get_author_social_urls(page)
                    page_data['facebook_url'] = social_link_urls['facebook']
                    page_data['twitter_url'] = social_link_urls['twitter']
                    page_data['linkedin_url'] = social_link_urls['linkedin']
                    page_data['google_url'] = social_link_urls['google']
                    page_data['website'] = get_author_website_url(page)

                json_string = json.dumps(page_data)
                outfile.write(json_string + '\n')

    outfile.close()


def main():
    print "GIVE MY CREATION LIFE"
    nodes_to_export = get_nodes_to_export_from_db()
    write_html_content_to_output(nodes_to_export)

if __name__ == "__main__":
    main()
