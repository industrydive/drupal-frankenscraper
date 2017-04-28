import MySQLdb
import settings
import sys
import urllib2
import json
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
    node_query = "select nid, changed from node where type = 'post' and status = 1 order by changed DESC limit 15"
    node_cursor = db.cursor()
    node_cursor.execute(node_query)
    nodes = []
    for nid, changed in node_cursor:
        node_data = {'nid': nid, 'changed': changed}

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


def get_pub_and_author_info(page):
    pub_date_div = page.body.find('div', {'class': 'field-name-post-date-author-name'})
    author_name = pub_date_div.p.a.text
    pub_date = pub_date_div.p.text.replace(author_name, '').strip()
    author_link = pub_date_div.p.a['href']
    return author_name, author_link, pub_date


def get_story_body(page):
    body_div = page.body.find('div', {'class': 'field-name-body'})
    body_content_div = body_div.find('div', {'property': 'content:encoded'})
    return body_content_div.decode_contents(formatter="html")


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
    outfile = open('output/story.jl', 'w+')
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
                node['pub_date']
            ) = get_pub_and_author_info(page)
            node['story_body'] = get_story_body(page)
            json_string = json.dumps(node)
            outfile.write(json_string + '\n')

    outfile.close()


def main():
    print "GIVE MY CREATION LIFE"
    nodes_to_export = get_nodes_to_export_from_db()
    write_html_content_to_output(nodes_to_export)

if __name__ == "__main__":
    main()
