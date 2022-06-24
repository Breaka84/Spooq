# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../../spooq'))
# sys.path[0:0] = [os.path.abspath('_themes/foundation-sphinx-theme')]

# import easydev
# from recommonmark.parser import CommonMarkParser
# from recommonmark.transform import AutoStructify
# import sphinx_bootstrap_theme
# import foundation_sphinx_theme
from spooq._version import __version__ as version_number

# -- Project information -----------------------------------------------------

project = u'Spooq'
copyright = u'2020, David Hohensinn'
author = u'David Hohensinn'

# The short X.Y version
version = u'3'
# The full version, including alpha/beta/rc tags
release = version_number


# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
#    'sphinx.ext.githubpages',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinxcontrib.plantuml',
    # 'easydev.copybutton',
    # 'foundation_sphinx_theme'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# Enable the Usage of Markdown for Sphinx


# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = ['.rst']
# source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = 'en'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path .
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
# pygments_style = 'sphinx'
pygments_style = 'default'

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
html_theme = 'sphinx_rtd_theme'
# html_style = 'css/make_content_wider.css'
# html_theme = 'groundwork'
# html_theme = 'agogo'
html_theme_options = {
    'body_max_width': '75%',
    # 'pagewidth': '75%',
    # 'documentwidth': '75%',
}

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.

#################
# bootstrap
#################
# html_theme = 'bootstrap'
# html_theme_path = sphinx_bootstrap_theme.get_html_theme_path()
#  Bootstrap Theme Specific
# html_theme_options = {
#     'navbar_site_name': 'Sitemap',
#     'navbar_pagenav_name': 'Page TOC',
# #    'bootswatch_theme' : 'slate',
#     'bootswatch_theme' : 'cerulean',
#     'bootstrap_version': '3'
# }

#################
# mozilla
#################
# import mozilla_sphinx_theme
# import os
# html_theme_path = [os.path.dirname(mozilla_sphinx_theme.__file__)]
# html_theme = 'mozilla'

#################
# foundation
#################
# html_theme = 'foundation_sphinx_theme'
# html_theme_path = foundation_sphinx_theme.HTML_THEME_PATH
# html_theme_options = {
#     'stylesheet': 'foundation/css/cards.css',
#     'motto': 'Spooq - Extracting, Transforming, and Loading on PySpark',
#     'author': u'David Eigenstuhler',
#     'copyright_year': '2020',
#     # 'github_ribbon_image': 'github-ribbon.png',
#     # 'seo_description': 'This is an example of the Foundation Sphinx Theme output.',
#     # 'github_user': 'peterhudec',
#     # 'github_repo': 'foundation-sphinx-theme',
#     # 'flattr_id': 'peterhudec',
# }

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = [
    'theme_overrides.css',
]

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
# The default sidebars (for documents that don't match any pattern) are
# defined by theme itself.  Builtin themes are using these templates by
# default: ``['localtoc.html', 'relations.html', 'sourcelink.html',
# 'searchbox.html']``.
#
# html_sidebars = {}


# -- Options for HTMLHelp output ---------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'Spooq_docs'


# -- Options for LaTeX output ------------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    'papersize': 'a4paper',
    # The font size ('10pt', '11pt' or '12pt').
    #
    'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    #
    'preamble': r"""
        \usepackage[columns=1]{idxlayout}
        \usepackage{geometry}
    """,
    # Latex figure (float) alignment
    #
    'figure_align': 'h!tbp',
    'classoptions': ', twoside',
    'babel': r'\usepackage[english]{babel}',
    'printindex': r'\raggedright\printindex',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'Spooq.tex', u'Spooq Documentation',
     u'David Eigenstuhler', 'manual'),
]
# latex_docclass = {'manual': 'scrbook'}


# -- Options for manual page output ------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'spooq', u'Spooq Documentation',
     [author], 1)
]


# -- Options for Texinfo output ----------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'Spooq', u'Spooq Documentation',
     author, 'Spooq', 'ETL library for Spark based Data Lakes.',
     'Miscellaneous'),
]

autodoc_member_order = 'bysource'
autodoc_default_flags = [
    'members',
    'show-inheritance',
]
autodoc_default_options = {
    'autoclass_content': 'class'
}

# -- Extension configuration -------------------------------------------------
add_module_names = False

# -- Options for todo extension ----------------------------------------------

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

intersphinx_mapping = {
    'python':         ('https://docs.python.org/3.8', None),
	# 'pyExceptions':   ('https://pyexceptions.readthedocs.io/en/stable/', None),
	# 'pyMetaClasses':  ('https://pymetaclasses.readthedocs.io/en/latest/', None),
    'pyspark':        ('https://spark.apache.org/docs/3.2.1/api/python/', None)

}

# app setup hook
git_doc_root = 'https://github.com/Breaka84/Spooq'

def skip(app, what, name, obj, would_skip, options):
    if name == "__init__":
        return True
    return would_skip

def setup(app):
    # app.add_config_value('recommonmark_config', {
    #     'url_resolver': lambda url: git_doc_root + url,
    #     'auto_toc_tree_section': 'Contents',
    #     'enable_eval_rst': True,
    #     'enable_auto_doc_ref': False,
    # }, True)
    # app.add_transform(AutoStructify)
    # app.connect("autodoc-skip-member", skip)
    # app.add_css_file("source/_static/make_content_wider.css")
    pass

# Configure PlantUML
plantuml_output_format = 'svg_img'
