from transformer import Transformer

class NoIdDropper(Transformer):
    """
    This is a simplified example on how to implement a new transformer class.
    Please take your time to write proper docstrings as they are automatically
    parsed via Sphinx to build the HTML and PDF documentation.
    Docstrings use the style of Numpy (via the napoleon plug-in).

    This class uses the :meth:`pyspark.sql.DataFrame.dropna` method internally.

    Examples
    --------
    input_df = some_extractor_instance.extract()
    transformed_df = NoIdDropper(
        id_columns='user_id'
    ).transform(input_df)

    Parameters
    ----------
    id_columns: :any:`str` or :any:`list`
        The name of the column containing the identifying Id values.
        Defaults to "id" 
    
    Raises
    ------
    :any:`exceptions.ValueError`: 
        "how ('" + how + "') should be 'any' or 'all'"
    :any:`exceptions.ValueError`: 
        "subset should be a list or tuple of column names"
    """

    def __init__(self, id_columns='id'):
        super(NoIdDropper, self).__init__()
        self.id_columns = id_columns


    def transform(self, input_df):
        self.logger.info("Dropping records without an Id (columns to consider: {col})"
            .format(col=self.id_columns))
        output_df = input_df.dropna(
            how='all', 
            thresh=None, 
            subset=self.id_columns
        )
        
        return output_df
