let
getFn = (limits as text, url as text, authQuery as record) =>
    let


/*
-------------------------------------
-------------Справочники-------------
-------------------------------------
*/
        //Запрос
        authWebContents = Web.Contents(
            url,
                [
                    RelativePath="/private/api/auth.php",
                    Query=authQuery
                ]),

                guideConnect = (url as text, authQuery as record) =>
    let
    getAccountInfo = Json.Document(Web.Contents(
        url,
        [
            RelativePath="/private/api/v2/json/accounts/current",
            Query=authQuery
        ])),
    getResponse = getAccountInfo[response],
    getResponse2 = getResponse[account]
in
    getResponse2,


        getAccountInfo = guideConnect(url, authQuery),

        //Имен пользователей
        usersRecord = getAccountInfo[users],
        usersToTable = Table.FromList(usersRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        usersExpandNames = Table.ExpandRecordColumn(usersToTable, "Column1", {"id", "name"}, {"id", "name"}),
        usersExpandNamesToText = Table.TransformColumnTypes(usersExpandNames,{{"id", type text}}),

        //Названий статусов
        statusesRecord = getAccountInfo[leads_statuses],
        statusesToTable = Table.FromList(statusesRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        statusesExpandNames = Table.ExpandRecordColumn(statusesToTable, "Column1", {"id", "name"}, {"id", "name"}),
        statusesChangeType = Table.TransformColumnTypes(statusesExpandNames,{{"id", type text}}),

        //Названий групп
        groupsRecord = getAccountInfo[groups],
        groupsToTable = Table.FromList(groupsRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        groupsCheckEmpty = Table.First(groupsToTable),
        groupsExpandNames = Table.ExpandRecordColumn(groupsToTable, "Column1", {"id", "name"}, {"id", "name"}),
        groupsChangeType = Table.TransformColumnTypes(groupsExpandNames,{{"id", type text}}),

        //пайплайны
        pipelinesRecord = getAccountInfo[pipelines],
        pipelinesToTable = Record.ToTable(pipelinesRecord),
        pipelinesCheckEmpty = Table.First(pipelinesToTable),
        pipelinesDelAnother = Table.SelectColumns(pipelinesToTable,{"Value"}),
        pipelinesExpandNames = Table.ExpandRecordColumn(pipelinesDelAnother, "Value", {"id", "name"}, {"id", "name"}),
        pipelinesChangeType = Table.TransformColumnTypes(pipelinesExpandNames,{{"id", type text}}),



        newAuthQuery = Record.Combine({
            authQuery,
            [limit_rows ="500"],
            [limit_offset=limits]}),

        getQuery  = Json.Document(Web.Contents(url,
            [
                RelativePath="/private/api/v2/json/leads/list",
                Query=newAuthQuery
            ])),
        toTable = Record.ToTable(getQuery),
        delOther = Table.SelectColumns(toTable,{"Value"}),
        expand = Table.ExpandRecordColumn(delOther, "Value", {"leads"}, {"leads"}),
        expand1 = Table.ExpandListColumn(expand, "leads"),
        expand2 = Table.ExpandRecordColumn(expand1, "leads", {"id", "name", "date_create", "created_user_id", "last_modified", "account_id", "price", "responsible_user_id", "linked_company_id", "group_id", "pipeline_id", "date_close", "closest_task", "loss_reason_id", "deleted", "tags", "status_id", "custom_fields", "main_contact_id"}, {"id", "name", "date_create", "created_user_id", "last_modified", "account_id", "price", "responsible_user_id", "linked_company_id", "group_id", "pipeline_id", "date_close", "closest_task", "loss_reason_id", "deleted", "tags", "status_id", "custom_fields", "main_contact_id"}),

        //Перевод дат из timestamp
        timestampDateCreate = Table.AddColumn(expand2, "Date_create", each if [date_create] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_create])),
        timestampDateModified = Table.AddColumn(timestampDateCreate, "Last_modified", each if [last_modified] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[last_modified])),
        timestampDateClose = Table.AddColumn(timestampDateModified, "Date_close", each if [date_close] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_close])),
        removeOldDates = Table.RemoveColumns(timestampDateClose,{"date_create", "last_modified", "date_close"}),
        removeOldDatesToText = Table.TransformColumnTypes(removeOldDates,{{"created_user_id", type text}, {"group_id", type text}, {"pipeline_id", type text}, {"status_id", type text}, {"responsible_user_id", type text}}),
        tagsNew = Table.AddColumn(removeOldDatesToText, "Tags.1", each Text.Combine(Table.FromRecords([tags])[name], ",")),

        //Справочник Custom_fields
        startCustomFields = Table.AddColumn(tagsNew, "Пользовательская", each Table.FromRecords([custom_fields])),
        delOtherCF = Table.SelectColumns(startCustomFields,{"id", "custom_fields"}),
        expandCF = Table.ExpandListColumn(delOtherCF, "custom_fields"),
        expandCF1 = Table.ExpandRecordColumn(expandCF, "custom_fields", {"name", "values"}, {"name", "values"}),
        addValuesCF = Table.AddColumn(expandCF1, "Пользовательская", each Text.Combine(Table.FromRecords([values])[value], ",")),
        delOtherCF1 = Table.RemoveColumns(addValuesCF,{"values"}),
        delNullCF = Table.SelectRows(delOtherCF1, each ([name] <> null)),
        finishCustomFields = Table.Pivot(delNullCF, List.Distinct(delNullCF[name]), "name", "Пользовательская"),

        //merge со справочниками
        mergeWithCustomFields = Table.NestedJoin(
            tagsNew,{"id"},
            finishCustomFields,{"id"},
            "CustomFields",JoinKind.LeftOuter),
        mergeWithCreateUserName = Table.NestedJoin(
            mergeWithCustomFields,{"created_user_id"},
            usersExpandNamesToText,{"id"},
            "CreatedUser",JoinKind.LeftOuter),
        mergeWithResponsibleUserName = Table.NestedJoin(
            mergeWithCreateUserName,{"responsible_user_id"},
            usersExpandNamesToText,{"id"},
            "ResponsibleUser",JoinKind.LeftOuter),
        mergeWithGroupsName = if groupsCheckEmpty = null
            then mergeWithResponsibleUserName
            else Table.NestedJoin(
                mergeWithResponsibleUserName,{"group_id"},
                groupsChangeType,{"id"},
                "GroupName",JoinKind.LeftOuter),
        mergeWithPipelineName = if pipelinesCheckEmpty = null
            then mergeWithGroupsName
            else Table.NestedJoin(
                mergeWithGroupsName,{"pipeline_id"},
                pipelinesChangeType,{"id"},
                "PipelineName",JoinKind.LeftOuter),
        mergeWithStatusName = Table.NestedJoin(
            mergeWithPipelineName,{"status_id"},
            statusesChangeType,{"id"},
            "StatusesName",JoinKind.LeftOuter),

        expandCreaterName = Table.ExpandTableColumn(mergeWithStatusName, "CreatedUser", {"name"}, {"CreatedUser.name"}),
        expandResponsibleName = Table.ExpandTableColumn(expandCreaterName, "ResponsibleUser", {"name"}, {"ResponsibleUser.name"}),
        expandPipelineName = if pipelinesCheckEmpty = null
            then expandResponsibleName
            else Table.ExpandTableColumn(expandResponsibleName, "PipelineName", {"name"}, {"PipelineName.name"}),
        expandStatusesName = Table.ExpandTableColumn(expandPipelineName, "StatusesName", {"name"}, {"StatusesName.name"}),
        expandGroupsName = if groupsCheckEmpty = null
            then expandStatusesName
            else Table.ExpandTableColumn(expandStatusesName, "GroupName", {"name"}, {"GroupName.name"}),

        delFinal = Table.RemoveColumns(expandGroupsName,{"created_user_id", "responsible_user_id", "group_id", "pipeline_id", "tags", "status_id", "custom_fields"}),
        renameFinal = Table.RenameColumns(delFinal,{{"Tags.1", "Tags"}})

in
    renameFinal
in
getFn
