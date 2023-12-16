def remove_newlines(document):
    cleaned_page_content = document.page_content.replace('\n', '')
    document.page_content = cleaned_page_content
    return document

def transform_data(data):
    return [remove_newlines(doc) for doc in data]
