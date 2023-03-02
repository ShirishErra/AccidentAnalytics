import pandas as pd
import sys
from helpers.helper import Helper
CONFIG = "config.yaml"
class Accident:
    def __init__(self, config, helper):

        paths = helper.read_config(config).get("Data")
        self.df_charges = helper.get_dataframe(paths.get("charges"))
        self.df_damages = helper.get_dataframe(paths.get("damages"))
        self.df_endorse = helper.get_dataframe(paths.get("endorses"))
        self.df_primary_person = helper.get_dataframe(paths.get("persons"))
        self.df_units = helper.get_dataframe(paths.get("units"))
        self.df_restrict = helper.get_dataframe(paths.get("restricts"))

    def no_of_accidents_with_male_killed(self):
        """
        The crashes (accidents) in which number of persons killed are male
        :param output_path: output file path
        :return: dataframe count
        """
        filter = (self.df_primary_person['PRSN_GNDR_ID'] == "MALE") & (self.df_primary_person['DEATH_CNT'] == 1)
        df = self.df_primary_person[filter]
        result = {'no_of_accidents_with_male_killed' : len(df.index)}
        return pd.Series(result)

    def no_of_accidents_where_2wheeler_involved(self):
        """
        The crashes where the vehicle type was 2 wheeler.
        :param output_path: output file path
        :return: dataframe count
        """
        df = self.df_units[self.df_units["VEH_BODY_STYL_ID"].str.contains("MOTORCYCLE", na=False)].drop_duplicates()
        result = {'no_of_accidents_where_2wheeler_involved':len(df.index)}

        return pd.Series(result)

    def get_state_with_highest_accidents_where_females_involved(self):
        """
        The state has highest number of accidents in which females are involved

        :param output_path: output file path
        :return: state name with highest female accidents
        """
        df_1 = self.df_primary_person[self.df_primary_person.PRSN_GNDR_ID == "FEMALE"].groupby('DRVR_LIC_STATE_ID').count().sort_values('PRSN_GNDR_ID', ascending=False).reset_index().head(1)
        result = {'state_with_highest_accidents_where_females_involved' : str(df_1['DRVR_LIC_STATE_ID'][0])}
        return pd.Series(result)
    def get_top_5to15_vehicles_involved_in_accidents(self):
        """
        The Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        :param output_path: output file path
        :return: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        self.df_units['TOT_CASUALTIES_CNT'] = self.df_units['TOT_INJRY_CNT'] + self.df_units['DEATH_CNT']

        top20 = self.df_units.groupby("VEH_MAKE_ID")['TOT_CASUALTIES_CNT'].sum().reset_index()\
            .sort_values('TOT_CASUALTIES_CNT', ascending=False).head(20)
        result = pd.Series(top20.iloc[4:14, 0]).reset_index()
        return pd.Series(result['VEH_MAKE_ID'])

    def get_top_ethnic_grp_in_each_body_styles(self):
        """
        The top ethnic user group of each unique body style, for all the body styles involved in crashes

        :param output_path: output path of the output to be stored
        :return: none
        """
        df = pd.merge(self.df_units, self.df_primary_person, on='CRASH_ID', how='inner')
        filter1 = df['VEH_BODY_STYL_ID'].isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])
        filter2 = df['PRSN_ETHNICITY_ID'].isin(["NA", "UNKNOWN"])
        df = df[(~filter1) & (~filter2)]

        df = df.drop_duplicates()

        df_1 = df.groupby(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"])['CRASH_ID'].count().reset_index()
        df_1['rank'] = df_1.sort_values(['CRASH_ID'], ascending=False).groupby(['VEH_BODY_STYL_ID']).cumcount() + 1
        df_1 = df_1[df_1['rank'] == 1].sort_values('CRASH_ID', ascending=False).reset_index()
        return pd.DataFrame(df_1[['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']])


    def get_top5_zip_accidents_with_alchohol_factor(self):
        """
        Among the crashed cars, the Top 5 Zip Codes with highest number crashes
        with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        :param output_path: output path of the output to be stored
        :return: none
        """
        df = pd.merge(self.df_units, self.df_primary_person, on='CRASH_ID', how='inner')
        df = df[df['DRVR_ZIP'].notna()]

        filter = (df['CONTRIB_FACTR_1_ID'].str.contains("ALCOHOL")) | (df['CONTRIB_FACTR_2_ID'].str.contains("ALCOHOL"))
        df = df[filter]

        df = df['DRVR_ZIP'].value_counts().reset_index()
        result = list(df.iloc[0:5, 0])

        return pd.Series(result)

    def get_crash_ids_with_insurance_and_no_damage(self):
        """
        Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        :param output_path: output file path
        :return: List of crash ids
        """

        df = self.df_damages.merge(self.df_units, on='CRASH_ID', how='inner')
        filter1 = ((df.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (~df.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
        filter2 = ((df.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (~df.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))

        df = df[filter1 | filter2]

        filter3 = (df.DAMAGED_PROPERTY == "NONE") & (df.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        df = df[filter3]
        crashids = df['CRASH_ID'].drop_duplicates()
        result = {
            'crash_ids_with_insurance_and_no_damage':len(crashids.index)
        }
        return pd.Series(result)

    def get_top_5_vehicle_brand_with_driver_offense_history(self):
        """
        The Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
        with highest number of offences (to be deduced from the data)

        :param output_path: output file path
        :return List of Vehicle brands
        """
        topstates = self.df_units["VEH_LIC_STATE_ID"].value_counts().reset_index()
        top25states = list(topstates.iloc[0:25, 0])

        topvcolors = self.df_units["VEH_COLOR_ID"].value_counts().reset_index()
        top10vcolors = list(topvcolors.iloc[0:10, 0])

        df = self.df_charges.merge(self.df_primary_person, on='CRASH_ID', how='inner')
        df = df.merge(self.df_units, on='CRASH_ID', how='inner')

        filter1 = df['CHARGE'].str.contains("SPEED", na=False)
        filter2 = df['DRVR_LIC_TYPE_ID'].isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])
        filter3 = df['VEH_COLOR_ID'].isin(top10vcolors)
        filter4 = df['VEH_LIC_STATE_ID'].isin(top25states)

        df = df[filter1 & filter2 & filter3 & filter4]
        result = df['VEH_MAKE_ID'].value_counts().reset_index()

        return pd.Series(list(result.iloc[0:5, 0]))


if __name__ == '__main__':
    config=''
    if len(sys.argv)<=1 or (sys.argv[1]!=CONFIG):
        while 1:
            file = input('Please provide the correct config file name to proceed further...!!!, Ex: config.yaml\n')
            if file==CONFIG:
                config=str(file)
                break
    else:
        config = str(sys.argv[1])

    helper = Helper()
    output_paths = helper.read_config(config).get("Output")
    accident = Accident(config, helper)

    print("########################### Execution Started ###################################")
    print("\n")
    answer1 = accident.no_of_accidents_with_male_killed()
    output_file1 = output_paths.get('analytics1')
    helper.dataframe_to_output(answer1, output_file1)
    print("-> Question-1 Finished ")

    answer2 = accident.no_of_accidents_where_2wheeler_involved()
    output_file2 = output_paths.get('analytics2')
    helper.dataframe_to_output(answer2, output_file2)
    print("-> Question-2 Finished")

    answer3 = accident.get_state_with_highest_accidents_where_females_involved()
    output_file3 = output_paths.get('analytics3')
    helper.dataframe_to_output(answer3, output_file3)
    print("-> Question-3 Finished")

    answer4 = accident.get_top_5to15_vehicles_involved_in_accidents()
    output_file4 = output_paths.get('analytics4')
    helper.dataframe_to_output(answer4, output_file4)
    print("-> Question-4 Finished")

    answer5 = accident.get_top_ethnic_grp_in_each_body_styles()
    output_file5 = output_paths.get('analytics5')
    helper.dataframe_to_output(answer5, output_file5)
    print("-> Question-5 Finished ")

    answer6 = accident.get_top5_zip_accidents_with_alchohol_factor()
    output_file6 = output_paths.get('analytics6')
    helper.dataframe_to_output(answer6, output_file6)
    print("-> Question-6 Finished ")

    answer7 = accident.get_crash_ids_with_insurance_and_no_damage()
    output_file7 = output_paths.get('analytics7')
    helper.dataframe_to_output(answer7, output_file7)
    print("-> Question-7 Finished ")

    answer8 = accident.get_top_5_vehicle_brand_with_driver_offense_history()
    output_file8 = output_paths.get('analytics8')
    helper.dataframe_to_output(answer8, output_file8)
    print("-> Question-8 Finished ")
    print("\n########################### Execution Ended ###################################")


