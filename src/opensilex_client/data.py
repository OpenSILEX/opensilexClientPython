import opensilexClientToolsPython as osCP



class _Data:

    def import_data(self, df, **kwargs):
        print("DATA DICT :", kwargs)
        print("DATA COLUMNS :", df.columns)
        BATCH_SIZE = 100

        df.value = df.value.fillna(value="NaN")

        data_dto_list = []
        for index,row in df.iterrows():
            new_dict = dict(row[[
                col
                for col in df.columns
                if col not in [x["uri"] for x in kwargs["prov_used"]]
            ]])

            new_dict["variable"] = kwargs["variable"]

            if "prov_used" in kwargs.keys():
                if "prov_was_associated_with" in kwargs.keys():
                    new_dict["provenance"] = osCP.DataProvenanceModel(
                        uri = kwargs["provenance"],
                        prov_used = [
                            osCP.ProvEntityModel(
                                uri=row[x["uri"]],
                                rdf_type=x["rdf_type"]
                            )
                            for x in kwargs["prov_used"]
                        ],
                        prov_was_associated_with=[
                            osCP.ProvEntityModel(
                                **x
                            )
                            for x in kwargs["prov_was_associated_with"]
                        ],
                        experiments=kwargs["experiments"] if "experiments" in kwargs.keys() else None
                    )
                else:
                    new_dict["provenance"] = osCP.DataProvenanceModel(
                        uri = kwargs["provenance"],
                        prov_used = [
                            osCP.ProvEntityModel(
                                uri=row[x["uri"]],
                                rdf_type=x["rdf_type"]
                            )
                            for x in kwargs["prov_used"]
                        ],
                        experiments=kwargs["experiments"] if "experiments" in kwargs.keys() else None
                    )
            elif "prov_was_associated_with" in kwargs.keys():
                new_dict["provenance"] = osCP.DataProvenanceModel(
                    uri = kwargs["provenance"],
                    prov_was_associated_with=[
                        osCP.ProvEntityModel(
                            **x
                        )
                        for x in kwargs["prov_was_associated_with"]
                    ],
                    experiments=kwargs["experiments"] if "experiments" in kwargs.keys() else None
                )
            
            data_dto_list.append(osCP.DataCreationDTO(**new_dict))


        if len(data_dto_list)%BATCH_SIZE == 0:
            n_batch = len(data_dto_list)//BATCH_SIZE
        else:
            n_batch = len(data_dto_list)//BATCH_SIZE + 1

        res = []
        for batch in range(n_batch):
            res += self.client._send_data(data_dto_list=data_dto_list[batch*BATCH_SIZE:(batch+1)*BATCH_SIZE])["result"]
        return res

    def import_datafile(self, df, **kwargs):
        print("DATAFILE DICT :", kwargs)
        print("DATAFILE COLUMNS :", df.columns)
        # TODO : prov_was_associated_with support
        # refactor -> if inside a single for loop
        res = []
        if "prov_used" in kwargs.keys():
            for index,row in df.iterrows():
                new_dict = dict(row[[
                    col
                    for col in df.columns
                    if col not in kwargs["prov_used"] and col != "file_name"
                ]])
                new_dict["rdf_type"] = kwargs["rdf_type"]
                new_dict["provenance"] = {
                    "uri" : kwargs["provenance"],
                    "experiments" : kwargs["experiments"], # TODO: this is just a quick fix it will crash if no experiment is given
                    "prov_used" : [
                        {"uri": row[x["uri"]], "rdf_type": x["rdf_type"]}
                        for x in kwargs["prov_used"]
                    ]
                }
                res.append(self.client._send_datafile(
                    description=new_dict,
                    file_path=row["file_name"]
                )["result"][0])
        else:
            for index,row in df.iterrows():
                new_dict = dict(row[[
                    col
                    for col in df.columns
                    if col != "file_name"
                ]])
                new_dict["rdf_type"] = kwargs["rdf_type"]
                new_dict["provenance"] = {
                    "uri" : kwargs["provenance"],
                    "experiments" : kwargs["experiments"] # TODO: this is just a quick fix it will crash if no experiment is given
                }
                res.append(self.client._send_datafile(
                    description=new_dict,
                    file_path=row["file_name"]
                )["result"][0])

        return res


    def import_data_or_files(self, df, **kwargs):
        if kwargs["type"] == "datafile":
            # Need to rename _date because in the case of datafiles
            # DTO objects aren't used. Everything is passed as txt.
            df = df.rename(columns={"_date":"date"})
            del kwargs["type"]
            return self.import_datafile(df, **kwargs)
        else:
            del kwargs["type"]
            # df2 = df.loc[:,[not "uri" in x for x in df.columns]]
            # print("DATA_DF :", df2)
            return self.import_data(df, **kwargs)
