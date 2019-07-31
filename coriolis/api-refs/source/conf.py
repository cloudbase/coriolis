
extensions = [
    'openstackdocstheme',
    'os_api_ref'
]

source_suffix = '.rst'

master_doc = 'index'

project = u'Coriolis API Reference'
copyright = u'2018-present, Cloudbase Solutions S.R.L'

repository_name = 'cloudbase/coriolis'
bug_project = 'coriolis'
bug_tag = 'api-ref'

pygments_style = 'sphinx'

html_theme = 'openstackdocs'

html_theme_options = {
    "sidebar_mode": "toc",
}

html_last_updated_fmt = '%Y-%m-%d %H:%M'

latex_documents = [
    ('index'),
]
