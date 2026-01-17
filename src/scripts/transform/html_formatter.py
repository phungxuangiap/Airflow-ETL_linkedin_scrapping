from bs4 import BeautifulSoup, Tag, NavigableString
import re

def check_single_html_tag(html: BeautifulSoup):
    return isinstance(html, Tag) and ((str(html).count('</') == 1 and str(html).count('/>')==0) or (str(html).count('/>')==1 and str(html).count('</') == 0))

def html_formatter(html_text:str):
    html_result= []
    arr_stack: list[BeautifulSoup] = []
    soup = BeautifulSoup(html_text.replace('\n', '').replace('<!---->', '').replace(' <!-- -->', ''), 'html.parser')
    root_tag: Tag = soup.find()
    if root_tag is None:
        print("Error: No HTML tag found in input")
        exit(1)
    
    root_tag_name = root_tag.name
    root_tag_attrs = root_tag.attrs
    if check_single_html_tag(soup):
        html_result.append(soup)
    else:
        arr_stack = list(reversed(list(soup.find().children)))
    arr_stack = [item for item in arr_stack if item != ' ']
    href_list = []
    while arr_stack:
        html_tag = arr_stack.pop()
        if isinstance(html_tag, Tag) and html_tag.get('href'):
            href_list.append(html_tag.get('href'))
        if isinstance(html_tag, Tag) and html_tag.has_attr('class'):
            del html_tag['class']
        if check_single_html_tag(html_tag):
            if html_tag.get_text(strip=True):
                html_result.append(html_tag)
        elif isinstance(html_tag, Tag):
            for html_item in reversed(list(html_tag.children)):
                arr_stack.append(html_item)
    new_soup = BeautifulSoup('', 'html.parser')
    
    if 'class' in root_tag_attrs:
        del root_tag_attrs['class']
    
    new_root = new_soup.new_tag(root_tag_name, attrs=root_tag_attrs)
    for tag in html_result:
        if isinstance(tag, Tag):
            if tag.has_attr('class'):
                del tag['class']
            new_root.append(tag)
        elif isinstance(tag, str) and tag.strip():
            new_root.append(tag)
    
    new_root = BeautifulSoup(str(new_root), 'html.parser').prettify()
    return new_root
        
