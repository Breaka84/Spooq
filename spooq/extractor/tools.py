from builtins import str
import os
import logging

spooq_logger = logging.getLogger("spooq")


def remove_hdfs_prefix(path):
    """Replaces references to HDFS from the input path

    Example
    -------
    >>> _remove_hdfs_prefix(u'hdfs://nameservice-ha:8020/user/furia_salamandra_faerfax/data')
    u'/user/furia_salamandra_faerfax/data'
    """
    cleaned_path = path.replace("hdfs://nameservice-ha", "").replace(":8020", "")
    spooq_logger.debug("remove_hdfs_prefix: input: {inp}, output: {outp}".format(inp=path, outp=cleaned_path))
    return cleaned_path


def fix_suffix(path):
    """
    Ensures Full Input Path ends with ``/*``

    Example
    -------
    >>> _fix_suffix(u'/user/furia_salamandra_faerfax/data/18/01/01')
    u'/user/furia_salamandra_faerfax/data/18/01/01/*'

    """
    if path[-2:] == "/*":
        cleaned_path = path
    else:
        cleaned_path = os.path.join(path, "*")
    spooq_logger.debug("fix_suffix: input: {inp}, output: {outp}".format(inp=path, outp=cleaned_path))
    return cleaned_path


def infer_input_path_from_partition(base_path=None, partition=None):
    """
    Derives the final input_path from the cleaned :py:data:`base_path` & :py:data:`partition`
    and returns it.

    Parameters
    ----------
    base_path : :any:`str`
    partition : :any:`str` or :any:`int`

    Returns
    -------
    :any:`str`
        The final input_path to be used for Extraction.

    See Also
    --------
    _clean_path
    """
    base_path = remove_hdfs_prefix(base_path)
    partition = str(partition)
    inferred_path = "{base_path}/{yy}/{mm}/{dd}/*".format(
        base_path=base_path, yy=partition[2:4], mm=partition[4:6], dd=partition[6:8]
    )
    spooq_logger.debug(
        "infer_input_path_from_partition: input: {inp}, output: {outp}".format(inp=base_path, outp=inferred_path)
    )
    return inferred_path
