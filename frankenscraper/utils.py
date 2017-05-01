import MySQLdb
import string
import urllib2
import time
import os
import settings
import json
import logging
from bs4 import BeautifulSoup


def set_up_files_and_logger():
    file_time = time.strftime("%Y-%m-%d_%H:%M:%S")
    out_dir = 'output/%s' % file_time
    os.makedirs(out_dir)

    outfile_story = open('%s/story.jl' % out_dir, 'w+')
    outfile_user = open('%s/user.jl' % out_dir, 'w+')
    outfile_log_name = '%s/frankenscrape.log' % out_dir

    return outfile_story, outfile_user, outfile_log_name


def get_db_connection():
    """ Establish a mysql database connection from given settings file values
        Return the database connection object
    """
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

    logging.debug("Connecting to mysql: %s" % kwargs)
    db = MySQLdb.connect(**kwargs)
    return db


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


def get_user_db_data(username, db):
    # derive the UID from the source URL for the profile:
    user_url_alias_query = (
        "select substring(source, 6) as uid from url_alias "
        "where alias = 'users/%s'" % username
    )
    user_url_alias_cursor = db.cursor()
    user_url_alias_cursor.execute(user_url_alias_query)
    uid = None
    for uid in user_url_alias_cursor:
        uid = uid
    if uid:
        user_query = "select uid, mail from users where uid = %s" % uid
        user_cursor = db.cursor()
        user_cursor.execute(user_query)
        for uid, mail in user_cursor:
            return uid, mail
    return None, None


def get_pub_and_author_info(page):
    pub_date_div = page.body.find(
        'div',
        {'class': 'field-name-post-date-author-name'}
    )
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
        target_content_div = target_div.find(
            'div', {'class': 'field-item even'}
        )
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
        link_div = page.body.find(
            'div',
            {'class': 'field-name-field-user-%s-url' % link_id}
        )
        if link_div:
            link_content_div = link_div.find(
                'div',
                {'class': 'field-item even'}
            )
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


def get_nodes_to_export_from_db(changed_epoch, db, args):
    # this query gets back the identifying information for the posts we want
    # as well as the node and full path URLs that we can use to get the current
    # actual HTML page
    logging.info("exporting from %s" % changed_epoch)
    node_query = (
        "select n.nid, n.changed, n.type as content_type, u.alias, u.source "
        "from node n join url_alias u on u.source = CONCAT('node/', n.nid) "
        "where n.type ='post' and n.status = 1 "
        "and u.pid=(select pid from url_alias where source = u.source "
        "order by pid desc limit 1) "
        "and n.changed > %d "
        "order by n.changed ASC "
    )
    logging.debug("using query: %s" % node_query)
    node_query = node_query % changed_epoch
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


def write_html_content_to_output(db, nodes_to_export, outfile_story,
                                 outfile_user):
    author_links = []
    story_success_count = 0
    user_success_count = 0
    error_count = 0
    highest_epoch = 0
    logging.debug("Starting export")
    for node in nodes_to_export:
        full_url = settings.site_url + '/' + node['url_path']
        page, error = get_page(full_url)
        logging.debug("exporting node %s at url %s" % (
            node['nid'], node['url_path'])
        )

        if page:
            try:
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

                    uid, email = get_user_db_data(node['author_username'], db)
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
                        error_data = {
                            'object_type': 'user',
                            'type': 'HTTP error',
                            'url': node['author_link'],
                            'error': user_page_error,
                        }
                        error_data_string = json.dumps(error_data)
                        logging.error(error_data_string)
                        error_count += 1

                    # we want the user_page_data written outside of the if user_page
                    # block because a user needs to be created to link the story
                    # object to even if we can't get their profile right now.
                    json_string = json.dumps(user_page_data)
                    outfile_user.write(json_string + '\n')

                story_success_count += 1
                if int(node['changed']) > highest_epoch:
                    epoch_changed_file = open('.highest_changed_epoch', 'w+')
                    epoch_changed_file.write(str(node['changed']))
                    epoch_changed_file.close()
                    highest_epoch = int(node['changed'])
            except Exception, e:
                error_data = {
                    'object_type': 'story',
                    'type': 'parsing error',
                    'url': full_url,
                    'error': str(e),
                }
                error_data_string = json.dumps(error_data)
                logging.error(error_data_string)
                error_count += 1
        else:
            error_data = {
                'object_type': 'story',
                'type': 'HTTP error',
                'url': full_url,
                'error': error,
            }
            error_data_string = json.dumps(error_data)
            logging.error(error_data_string)
            error_count += 1

    return story_success_count, user_success_count, error_count
