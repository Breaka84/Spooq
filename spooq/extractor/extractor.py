"""
Extractors are used to fetch, extract and convert a source data set into a PySpark DataFrame.
Exemplary extraction sources are **JSON Files** on file systems like HDFS, DBFS or EXT4
and relational database systems via **JDBC**.
"""

from builtins import object
import logging


class Extractor(object):
    """
    Base Class of Extractor Classes.

    Attributes
    ----------
    name : :any:`str`
        Sets the `__name__` of the class' type as `name`, which is essentially the Class' Name.
    logger : :any:`logging.Logger`
        Shared, class level logger for all instances.

    """

    def __init__(self):
        self.name = type(self).__name__
        self.logger = logging.getLogger("spooq")

    def extract(self):
        """
        Extracts Data from a Source and converts it into a PySpark DataFrame.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`

        Note
        ----
        This method does not take ANY input parameters. All needed parameters are defined
        in the initialization of the Extractor Object.
        """
        raise NotImplementedError("This method has to be implemented in the subclasses")

    def __str__(self):
        return "Extractor Object of Class {nm}".format(nm=self.name)
