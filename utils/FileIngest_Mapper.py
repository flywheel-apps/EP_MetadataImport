import pandas as pd
import numpy as np
import flywheel


class FileIngest_Mapper:
    def __init__(
            self,
            group_column=None,
            project_column=None,
            subject_column=None,
            session_column=None,
            acquisition_column=None,
            analysis_column=None,
            file_column=None,
            import_columns="ALL",
    ):

        self.subject_column = subject_column
        self.session_column = session_column
        self.acquisition_column = acquisition_column
        self.analysis_column = analysis_column
        self.file_column = file_column
        self.project_column = project_column
        self.group_column = group_column
        self.import_columns = import_columns
        self.fw = flywheel.Client()

    def map_data(self, data, namespace):
        """

        Args:
            data (pandas.Dataframe): a dataframe containing metadata to import

        Returns: mappers (list): a list of mapped metadata items

        """

        data.fillna("", inplace=True)
        nrows, ncols = data.shape
        log.info("Starting Mapping")

        data["Gear_Status"] = "Failed"
        data["Gear_FW_Location"] = None

        success_counter = 0

        mappers = []
        for row in range(nrows):
            data_row = data.iloc[row]
            # print(data_row)
            group = panda_pop(data_row, self.group_column)
            project = panda_pop(data_row, self.project_column)
            subject = panda_pop(data_row, self.subject_column)
            session = panda_pop(data_row, self.session_column)
            acquisition = panda_pop(data_row, self.acquisition_column)
            analysis = panda_pop(data_row, self.analysis_column)
            file = panda_pop(data_row, self.file_column)

            if self.import_columns == "ALL":
                import_data = data_row
            else:
                import_data = data.get(self.import_columns)

            mappers.append(
                DataMap(
                    fw=self.fw,
                    data=import_data,
                    group=group,
                    project=project,
                    subject=subject,
                    session=session,
                    acquisition=acquisition,
                    analysis=analysis,
                    file=file,
                    namespace=namespace,
                )
            )
        return mappers


def panda_pop(series, key, default=None):
    """recreate the behavior of a dictionary "pop" for a pandas series

    behavior:
    if element exists, return the value and remove the element
    if the element doesn't exist, return the default
    the default... uh... default is "None"

    Args:
        series (pandas.Series): The series to pop from
        key (string): the key to look for and pop
        default (anything): the default value to return if the key isn't present

    Returns:

    """
    if key in series:
        return series.pop(key)
    else:
        return default
