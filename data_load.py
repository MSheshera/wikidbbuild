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

def isWanted(title, categories):
    """
    Looks at the prefix and categories of the page title and says true 
    if it is a normal article page.
    """
    # Non-article pages have a prefix ending in ':' in their title
    unwanted_re = u'[a-z\s]+\:.+'
    dis_re = u'.*disambiguation.*'
    obs_re = u'.*Complete list of encyclopedia topics \(obsolete\)'

    want = True
    
    # Check for disambiguation pages
    for cat in categories:
        if re.match(pattern=dis_re, string=cat, flags=re.IGNORECASE) or re.match(pattern=obs_re, string=cat, flags=re.IGNORECASE):
            want = False        
    
    # Check for other unwanted pages with prefixes in their names.            
    if re.match(pattern=unwanted_re, string=title, flags=re.IGNORECASE):
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
        "(page_id, page_title) "
        "VALUES (%s, %s)")
    for key, val in article_dict.iteritems():
        article_data = (key, val[0])
        cursor.execute(add_article, article_data)
    cnx.commit()
  
    add_article_path = ("INSERT INTO path_tb "
        "(page_id, content_path, template_path, category_path) "
        "VALUES (%s, %s, %s, %s)")
    for key, val in article_dict.iteritems():
        article_path_data = (key, val[1], val[2], val[3])
        cursor.execute(add_article_path, article_path_data)
    cnx.commit()
    cursor.close()


    cnx.close()

def save_all_article_data(page_obj, rand_pid):
    """
    Write all article related content to disk. The content, the categories,
    and the templates. Writing all content to file as pickle dumps so that
    they can be read back in just like that :) 
    """
    
    paths = []
    paths.append(unicode(page_obj.title, 'utf-8'))

    content = page_obj.getWikiText()
    f_path = CONTENT_PATH + u"/" + rand_pid + CONTENT_SUFFIX + u".pd"
    with open(f_path,'w') as cf:
        pickle.dump(unicode(content,'utf-8'), cf)
    paths.append(f_path)

    infobox_template = getInfoboxes(page_obj.getTemplates())
    f_path = TEMPLATE_PATH + u"/" + rand_pid + TEMPLATE_SUFFIX + u".pd"
    with open(f_path,'w') as itf:
        pickle.dump(infobox_template, itf)
    paths.append(f_path)

    category = page_obj.getCategories()
    f_path = CATEGORY_PATH + u"/" + rand_pid + CATEGORY_SUFFIX + u".pd"
    with open(f_path,'w') as caf:
        pickle.dump(category, caf)
    paths.append(f_path)

    return paths


def fill_article_table(num_art_want):
    """
    Getting the data on my own.
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
                if isWanted(title=p.title, categories=p.categories):
                    count += 1
                    print p.title
                    rand_pid = unicode(str(rand_pid),'utf-8')
                    data = save_all_article_data(p, rand_pid)

                    art_dict[rand_pid] = data
                    
            except wiki.WikiError:
                pass
    populate_tables(art_dict)

if __name__ == '__main__':
    templates = [u'Template:1x', u'Template:;', u'Template:Ambox', u'Template:Anglicise rank', u'Template:COLON', u'Template:Category handler', u'Template:Citation needed', u'Template:Cite book', u'Template:Cite journal', u'Template:Colon', u'Template:Column-width', u'Template:Commons', u'Template:Commons category', u'Template:Delink', u'Template:Expand section', u'Template:Fix', u'Template:Fix/category', u'Template:Italic title', u'Template:Lpsn', u'Template:Main', u'Template:Main article', u'Template:PAGENAMEBASE', u'Template:Reflist', u'Template:Side box', u'Template:Sister project', u'Template:Str find', u'Template:Str left', u'Template:Taxobox', u'Template:Taxobox/Error colour', u'Template:Taxobox/core', u'Template:Taxobox colour', u'Template:Taxobox name', u'Template:Taxonomy', u'Template:Taxonomy/nobreak', u'Module:Arguments', u'Module:Category handler', u'Module:Category handler/blacklist', u'Module:Category handler/config', u'Module:Category handler/data', u'Module:Category handler/shared', u'Module:Citation/CS1', u'Module:Citation/CS1/COinS', u'Module:Citation/CS1/Configuration', u'Module:Citation/CS1/Date validation', u'Module:Citation/CS1/Identifiers', u'Module:Citation/CS1/Utilities', u'Module:Citation/CS1/Whitelist', u'Module:Delink', u'Module:Hatnote', u'Module:Hatnote list', u'Module:InfoboxImage', u'Module:Italic title', u'Module:Main', u'Module:Message box', u'Module:Message box/configuration', u'Module:Namespace detect/config', u'Module:Namespace detect/data', u'Module:No globals', u'Module:Side box', u'Module:String', u'Module:Unsubst', u'Module:Yesno']
    templates1 = [u'Template:Category handler', u'Template:Cite web', u'Template:Column-count', u'Template:Commons', u'Template:Commons category', u'Template:Convert', u'Template:DMCA', u'Template:Dated maintenance category', u'Template:Dead link', u'Template:FULLROOTPAGENAME', u'Template:Fix', u'Template:Fix/category', u'Template:HMS', u'Template:Ifsubst', u'Template:Infobox ship begin', u'Template:Infobox ship career', u'Template:Infobox ship characteristics', u'Template:Infobox ship image', u'Template:Main other', u'Template:Military navigation', u'Template:NVR', u'Template:Navbox', u'Template:Ns has subpages', u'Template:Other ships', u'Template:Reflist', u'Template:Sclass-', u'Template:Sclass/core', u'Template:Ship', u'Template:Ship in active service', u'Template:Side box', u'Template:Sister project', u'Template:Ticonderoga class cruiser', u'Template:Ticonderoga class cruiser armament mark 41', u'Template:Ticonderoga class cruiser beam', u'Template:Ticonderoga class cruiser complement', u'Template:Ticonderoga class cruiser displacement', u'Template:Ticonderoga class cruiser draft', u'Template:Ticonderoga class cruiser draught', u'Template:Ticonderoga class cruiser length', u'Template:Ticonderoga class cruiser propulsion', u'Template:Ticonderoga class cruiser sensors', u'Template:Ticonderoga class cruiser speed', u'Template:USN flag', u'Template:USS', u'Template:Use mdy dates', u'Template:WPMILHIST Infobox style', u'Module:Arguments', u'Module:Category handler', u'Module:Category handler/blacklist', u'Module:Category handler/config', u'Module:Category handler/data', u'Module:Category handler/shared', u'Module:Citation/CS1', u'Module:Citation/CS1/COinS', u'Module:Citation/CS1/Configuration', u'Module:Citation/CS1/Date validation', u'Module:Citation/CS1/Identifiers', u'Module:Citation/CS1/Utilities', u'Module:Citation/CS1/Whitelist', u'Module:Convert', u'Module:Convert/data', u'Module:Convert/text', u'Module:Data', u'Module:Hatnote', u'Module:Hatnote list', u'Module:InfoboxImage', u'Module:Namespace detect/config', u'Module:Namespace detect/data', u'Module:Navbar', u'Module:Navbox', u'Module:No globals', u'Module:Ns has subpages', u'Module:Other ships', u'Module:Other uses', u'Module:Side box', u'Module:TableTools', u'Module:Unsubst', u'Module:WPMILHIST Infobox style', u'Module:WPSHIPS utilities', u'Module:Yesno']
    templates2 = [u'Template:About', u'Template:Annulenes', u'Template:Authority control', u'Template:Border-radius', u'Template:Box-shadow', u'Template:Cascite', u'Template:Category handler', u'Template:Chem', u'Template:Chem/atom', u'Template:Chem/link', u'Template:ChemID', u'Template:Chem molar mass', u'Template:Chem molar mass/format', u'Template:Chembox', u'Template:Chembox AllOtherNames', u'Template:Chembox AllOtherNames/format', u'Template:Chembox Appearance', u'Template:Chembox AutoignitionPt', u'Template:Chembox BoilingPt', u'Template:Chembox CASNo', u'Template:Chembox CASNo/format', u'Template:Chembox CASNo/parametercheck', u'Template:Chembox CalcTemperatures', u'Template:Chembox ChEBI', u'Template:Chembox ChEBI/format', u'Template:Chembox ChEMBL', u'Template:Chembox ChEMBL/format', u'Template:Chembox ChemSpiderID', u'Template:Chembox ChemSpiderID/format', u'Template:Chembox DeltaHc', u'Template:Chembox DeltaHf', u'Template:Chembox Density', u'Template:Chembox Dipole', u'Template:Chembox ECHA', u'Template:Chembox ECNumber', u'Template:Chembox EUClass', u'Template:Chembox Elements', u'Template:Chembox Elements/molecular formula', u'Template:Chembox Entropy', u'Template:Chembox ExploLimits', u'Template:Chembox FlashPt', u'Template:Chembox Footer', u'Template:Chembox GHSPictograms', u'Template:Chembox GHSSignalWord', u'Template:Chembox HPhrases', u'Template:Chembox Hazards', u'Template:Chembox HeatCapacity', u'Template:Chembox Identifiers', u'Template:Chembox InChI', u'Template:Chembox InChI/format', u'Template:Chembox Jmol', u'Template:Chembox Jmol/format/sandbox', u'Template:Chembox KEGG', u'Template:Chembox KEGG/format', u'Template:Chembox LambdaMax', u'Template:Chembox Lethal amounts (set)', u'Template:Chembox LogP', u'Template:Chembox MagSus', u'Template:Chembox MainHazards', u'Template:Chembox MeltingPt', u'Template:Chembox MolShape', u'Template:Chembox NFPA', u'Template:Chembox NIOSH (set)', u'Template:Chembox Odour', u'Template:Chembox OtherCpds', u'Template:Chembox PPhrases', u'Template:Chembox Properties', u'Template:Chembox PubChem', u'Template:Chembox PubChem/format', u'Template:Chembox RPhrases', u'Template:Chembox RTECS', u'Template:Chembox RefractIndex', u'Template:Chembox Related', u'Template:Chembox SDS', u'Template:Chembox SMILES', u'Template:Chembox SMILES/format', u'Template:Chembox SPhrases', u'Template:Chembox Solubility', u'Template:Chembox SolubilityInWater', u'Template:Chembox Structure', u'Template:Chembox Supplement', u'Template:Chembox Thermochemistry', u'Template:Chembox UNII', u'Template:Chembox UNII/format', u'Template:Chembox VaporPressure', u'Template:Chembox Viscosity', u'Template:Chembox headerbar', u'Template:Chembox image', u'Template:Chembox image cell', u'Template:Chembox image sbs', u'Template:Chembox image sbs cell', u'Template:Chembox removeInitialLinebreak', u'Template:Chembox subDatarow', u'Template:Chembox subHeader', u'Template:Chembox templatePar/formatPreviewMessage', u'Template:Chembox verification', u'Template:Chemboximage', u'Template:Chemspidercite', u'Template:Citation', u'Template:Citation needed', u'Template:Cite book', u'Template:Cite journal', u'Template:Cite web', u'Template:Collapsible list', u'Template:Column-width', u'Template:Commons', u'Template:Commons category', u'Template:Convert', u'Template:Cross', u'Template:Cycloalkenes', u'Template:DOI', u'Template:Delink', u'Template:Distinguish', u'Template:Doi', u'Template:Ebicite', u'Template:EditAtWikidata', u'Template:Eqm', u'Template:Fdacite', u'Template:Fix', u'Template:Fix/category', u'Template:Functional Groups', u'Template:Functional group', u'Template:GABAAR PAMs', u'Template:GHS02', u'Template:GHS07', u'Template:GHS08', u'Template:GHS exclamation mark', u'Template:GHS flame', u'Template:GHS health hazard', u'Template:GHS phrases format', u'Template:H-phrase text', u'Template:H-phrases', u'Template:Hazchem F', u'Template:Hazchem T', u'Template:Hide in print', u'Template:Hydrocarbons', u'Template:IDLH', u'Template:Icon', u'Template:Keggcite', u'Template:Key press', u'Template:Key press/core', u'Template:Keypress', u'Template:Linear-gradient', u'Template:Longitem', u'Template:Main', u'Template:Main article', u'Template:Main other', u'Template:Molecules detected in outer space', u'Template:NFPA 704 diamond', u'Template:NFPA 704 diamond/text', u'Template:Navbox', u'Template:Navbox generic', u'Template:Navbox subgroup', u'Template:Navbox subgroups', u'Template:Nist', u'Template:Nowrap', u'Template:Only in print', u'Template:P-phrase text', u'Template:P-phrases', u'Template:PGCH', u'Template:ParmPart', u'Template:Pp-move-vandalism', u'Template:PubChem', u'Template:R-phrase', u'Template:R11', u'Template:R16', u'Template:R36/38', u'Template:R45', u'Template:R46', u'Template:R48/23/24/25', u'Template:R65', u'Template:Reflist', u'Template:RubberBible86th', u'Template:S45', u'Template:S53', u'Template:Side box', u'Template:Sister project', u'Template:Stdinchicite', u'Template:Str left', u'Template:Su', u'Template:Tick', u'Template:Trim', u'Template:Wikiquote', u'Template:Wiktionary', u'Template:Yesno', u'Module:About', u'Module:Arguments', u'Module:Authority control', u'Module:Category handler', u'Module:Category handler/blacklist', u'Module:Category handler/config', u'Module:Category handler/data', u'Module:Category handler/shared', u'Module:Check for unknown parameters', u'Module:Citation/CS1', u'Module:Citation/CS1/COinS', u'Module:Citation/CS1/Configuration', u'Module:Citation/CS1/Date validation', u'Module:Citation/CS1/Identifiers', u'Module:Citation/CS1/Utilities', u'Module:Citation/CS1/Whitelist', u'Module:Collapsible list', u'Module:Convert', u'Module:Convert/data', u'Module:Convert/text', u'Module:Delink', u'Module:Distinguish', u'Module:EditAtWikidata', u'Module:Effective protection expiry', u'Module:Effective protection level', u'Module:File link', u'Module:Hatnote', u'Module:Hatnote list', u'Module:Icon', u'Module:Icon/data', u'Module:InfoboxImage', u'Module:Main', u'Module:Math', u'Module:Namespace detect/config', u'Module:Namespace detect/data', u'Module:Navbar', u'Module:Navbox', u'Module:No globals', u'Module:ParameterCount', u'Module:Protection banner', u'Module:Protection banner/config', u'Module:Side box', u'Module:String', u'Module:StringReplace', u'Module:Su', u'Module:TableTools', u'Module:TemplatePar', u'Module:Unsubst', u'Module:Wikidata', u'Module:Wikidata/i18n', u'Module:Yesno']
    #print getInfoboxes(templates)
    fill_article_table(num_art_want=1200)