from wikitools import wiki, page
import random
import re
import pickle
import pprint 
from collections import *
import mysql.connector
from mysql.connector import errorcode

CONTENT_PATH = u"/home/msheshera/MSS/Code/Projects/CS585/post_progress/database_build/content"
CATEGORY_PATH = u"/home/msheshera/MSS/Code/Projects/CS585/post_progress/database_build/category"
TEMPLATE_PATH = u"/home/msheshera/MSS/Code/Projects/CS585/post_progress/database_build/template"
CONTENT_SUFFIX = u"_content"
TEMPLATE_SUFFIX = u"_template"
CATEGORY_SUFFIX = u"_category"

def isWanted(page_obj, multi):
    """
    Looks at the prefix and categories of the page title and says true 
    if it is a normal article page.
    """
    # If its for multi then you want to say false if there are no templates
    # in use.
    if multi is True:
    	if len(page_obj.templates) == 0:
    		return False

    # Non-article pages have a prefix ending in ':' in their title		
    unwanted_re = u'[a-z\s]+\:.+'
    dis_re = u'.*disambiguation.*'
    obs_re = u'.*Complete list of encyclopedia topics \(obsolete\)'

    want = True
    
    # Check for disambiguation pages
    for cat in page_obj.categories:
        if re.match(pattern=dis_re, string=cat, flags=re.IGNORECASE) or re.match(pattern=obs_re, string=cat, flags=re.IGNORECASE):
            want = False        
    
    # Check for other unwanted pages with prefixes in their names.            
    if re.match(pattern=unwanted_re, string=page_obj.title, flags=re.IGNORECASE):
        want = False
    
    return want

def getInfoboxes(templates):
    """
    Check if there are any infoboxes. Return if there are.
    """
    
    infobox_re = u'Template:\s*((infobox)\s+[a-z0-9\s\.\(\)\-]+)'
    other_infobox_re = u'Template:\s*((taxobox|gnf protein box|speciesbox|chembox))\s+[a-z0-9\s\.\(\)\-]+'
    infoboxes = []
    
    for template in templates:
        mo_i = re.match(pattern=infobox_re, string=template, flags=re.IGNORECASE)
        mo_o = re.match(pattern=other_infobox_re, string=template, flags=re.IGNORECASE)
        # If the infobox used is an "other" infobox then just return the type
        # for example if its 'taxobox' then just return 'taxobox' instead of
        # 'taxobox image' etc etc. Also assuming that an article will have
        # only normal infoboxes or the other kind of infoboxes.
        if mo_i:
            mo = mo_i
        elif mo_o:
            mo = mo_o
        else:
            mo = mo_i

        if mo:
            temp = mo.group(1).lower()
            if temp not in infoboxes:
                infoboxes.append(temp)
    
    return infoboxes

def populate_tables(article_dict):

    """
    article_tb: has the page id and the title for each article.
    path_tb: has the paths to the content, cataegories and the templates
    """
    cnx = mysql.connector.connect(user='root', password='rootpassword4186', host='127.0.0.1', database='nlp2016')
    cursor = cnx.cursor()
    
    add_article = ("INSERT INTO article_tb "
        "(page_id, page_title, needs) "
        "VALUES (%s, %s, %s)")
    for key, val in article_dict.iteritems():
        article_data = [key, val[0], val[1]]
        cursor.execute(add_article, article_data)
    cnx.commit()
  
    add_article_path = ("INSERT INTO path_tb "
        "(page_id, content_path, template_path, category_path) "
        "VALUES (%s, %s, %s, %s)")
    for key, val in article_dict.iteritems():
        article_path_data = [key, val[3], val[2], val[4]]
        cursor.execute(add_article_path, article_path_data)
    cnx.commit()
    cursor.close()


    cnx.close()

def save_all_article_data(page_obj, rand_pid):
    """
    Write all article related content to disk. The content, the categories,
    and the templates. Writing all content to file as pickle dumps so that
    they can be read back in just like that :) 
    Returns:
    	paths_pp:[title,if_template,template_path,content_path,category_path]
    """
    
    paths_pp = []
    paths_pp.append(unicode(page_obj.title, 'utf-8'))

    # Already filtered for infoboxes at this point.
    infobox_template = page_obj.templates
    f_path = TEMPLATE_PATH + u"/" + rand_pid + TEMPLATE_SUFFIX + u".pd"
    with open(f_path,'w') as itf:
        pickle.dump(infobox_template, itf)
    # Annotation help; If the article has a template then needs=1.
    if len(infobox_template) == 0: 
    	paths_pp.append(0)
    else: 
    	paths_pp.append(1)
    
    paths_pp.append(f_path)

    content = page_obj.getWikiText()
    f_path = CONTENT_PATH + u"/" + rand_pid + CONTENT_SUFFIX + u".pd"
    with open(f_path,'w') as cf:
        pickle.dump(unicode(content,'utf-8'), cf)
    paths_pp.append(f_path)

    category = page_obj.getCategories()
    f_path = CATEGORY_PATH + u"/" + rand_pid + CATEGORY_SUFFIX + u".pd"
    with open(f_path,'w') as caf:
        pickle.dump(category, caf)
    paths_pp.append(f_path)
    return paths_pp


def fill_article_table(num_art_want, multi):
    """
    Write content to file and save paths etc to the database.
    When multi is true we're building the dataset for the multi-class
    classification task. Binary task otherwise.
    """
    
    site = wiki.Wiki(u"https://en.wikipedia.org/w/api.php")
    MIN_PID = 10 # Page ids start at 10
    MAX_PID = 50401510 # Set this to whatever within range
    NS = 0 # Main article namespace. https://goo.gl/Sa3yBC
    
    # Get num_art_want number of random articles
    count = 0
    art_dict = {}

    while count < num_art_want:
        rand_pid = random.randint(MIN_PID, MAX_PID)
        if rand_pid not in art_dict:
            try:
                p = page.Page(site=site, namespace=NS, pageid=rand_pid)

                # Ensuring that its not a page from a different namespace.
                # This shouldn't happen according to what the API 
                # documentation says. IDK why its happening.
                p.getCategories()
                # Over writing the page objects' list of templates with just 
                # the infobox templates; seems like a bad thing to do but idk.
                p.templates = getInfoboxes(p.getTemplates())

                if isWanted(p, multi=multi):
                    count += 1
                    print "dl:", p.title
                    rand_pid = unicode(str(rand_pid),'utf-8')
                    data = save_all_article_data(p, rand_pid)

                    art_dict[rand_pid] = data
                    
            except wiki.WikiError:
                pass
    populate_tables(art_dict)

if __name__ == '__main__':
       fill_article_table(num_art_want=2400, multi=False)
