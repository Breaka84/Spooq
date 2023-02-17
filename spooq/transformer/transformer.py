"""
Transformers take a |SPARK_DATAFRAME| as an input, transform it accordingly
and return a |SPARK_DATAFRAME|.

Each Transformer class has to have a `transform` method which takes no arguments
and returns a |SPARK_DATAFRAME|.

Possible transformation methods can be Selecting the most up-to-date record by id,
Exploding an array, Filter (on an exploded array), Apply basic threshold cleansing or
Map the incoming DataFrame to at provided structure.
"""

from builtins import object
import logging


class Transformer(object):
    """
    Base Class of Transformer Classes.

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

    def transform(self, input_df):
        """
        Performs a transformation on a DataFrame.

        Parameters
        ----------
        input_df : |SPARK_DATAFRAME|
            Input DataFrame

        Returns
        -------
        |SPARK_DATAFRAME|
            Transformed DataFrame.

        Note
        ----
        This method does only take the Input DataFrame as a parameters. Any other needed parameters
        are defined in the initialization of the Transformator Object.
        """
        raise NotImplementedError("This method has to be implemented in the subclasses")

    def __str__(self):
        return "Transformer Object of Class {nm}".format(nm=self.name)
