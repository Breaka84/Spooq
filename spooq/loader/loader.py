"""
Loaders take a :py:class:`pyspark.sql.DataFrame` as an input and save it to a sink.

Each Loader class has to have a `load` method which takes a DataFrame as single paremter.

Possible Loader sinks can be **Hive Tables**, **Kudu Tables**, **HBase Tables**, **JDBC
Sinks** or **ParquetFiles**.
"""

from builtins import object
import logging


class Loader(object):
    """
    Base Class of Loader Objects.

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

    def load(self, input_df):
        """
        Persists data from a PySpark DataFrame to a target table.

        Parameters
        ----------
        input_df : :any:`pyspark.sql.DataFrame`
            Input DataFrame which has to be loaded to a target destination.

        Note
        ----
        This method takes only a single DataFrame as an input parameter. All other needed
        parameters are defined in the initialization of the Loader object.
        """
        raise NotImplementedError("This method has to be implemented in the subclasses")

    def __str__(self):
        return "Loader Object of Class {nm}".format(nm=self.name)
