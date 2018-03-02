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

        //Названий групп
        groupsRecord = getAccountInfo[groups],
        groupsToTable = Table.FromList(groupsRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        groupsCheckEmpty = Table.First(groupsToTable),
        groupsExpandNames = Table.ExpandRecordColumn(groupsToTable, "Column1", {"id", "name"}, {"id", "name"}),
        groupsChangeType = Table.TransformColumnTypes(groupsExpandNames,{{"id", type text}}),

        /*
-------------------------------------
-------------Справочники-------------
-------------------------------------
*/



        newAuthQuery = Record.Combine({
            authQuery,
            [limit_rows ="500"],
            [limit_offset=limits]}),

        getQuery  = Json.Document(Web.Contents(url,
            [
                RelativePath="/private/api/v2/json/contacts/list",
                Query=newAuthQuery
            ])),
        toTable = Record.ToTable(getQuery),
        delOther = Table.SelectColumns(toTable,{"Value"}),
        expand = Table.ExpandRecordColumn(delOther, "Value", {"contacts"}, {"contacts"}),
        expand1 = Table.ExpandListColumn(expand, "contacts"),
        expand2 = Table.ExpandRecordColumn(expand1, "contacts", {"id", "name", "last_modified", "account_id", "date_create", "created_user_id", "modified_user_id", "responsible_user_id", "group_id", "closest_task", "linked_company_id", "company_name", "tags", "type", "custom_fields", "linked_leads_id"}, {"id", "name", "last_modified", "account_id", "date_create", "created_user_id", "modified_user_id", "responsible_user_id", "group_id", "closest_task", "linked_company_id", "company_name", "tags", "type", "custom_fields", "linked_leads_id"}),

        //Перевод дат из timestamp
        timestampDateCreate = Table.AddColumn(expand2, "Date_create", each if [date_create] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_create])),
        timestampDateModified = Table.AddColumn(timestampDateCreate, "Last_modified", each if [last_modified] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[last_modified])),
        removeOldDates = Table.RemoveColumns(timestampDateModified,{"date_create", "last_modified"}),
        removeOldDatesToText = Table.TransformColumnTypes(removeOldDates,{{"created_user_id", type text}, {"modified_user_id", type text}, {"responsible_user_id", type text}, {"group_id", type text}}),

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
        mergeWithModifiedUserName = Table.NestedJoin(
            mergeWithResponsibleUserName,{"modified_user_id"},
            usersExpandNamesToText,{"id"},
            "ModifiedUser",JoinKind.LeftOuter),
        mergeWithGroupsName = if groupsCheckEmpty = null
            then mergeWithModifiedUserName
            else Table.NestedJoin(
                mergeWithModifiedUserName,{"group_id"},
                groupsChangeType,{"id"},
                "GroupName",JoinKind.LeftOuter),

        //expand
        expandCreaterName = Table.ExpandTableColumn(mergeWithGroupsName, "CreatedUser", {"name"}, {"CreatedUser.name"}),
        expandResponsibleName = Table.ExpandTableColumn(expandCreaterName, "ResponsibleUser", {"name"}, {"ResponsibleUser.name"}),
        expandModifiedName = Table.ExpandTableColumn(expandResponsibleName, "ModifiedUser", {"name"}, {"ModifiedUser.name"}),
        expandGroupsName = if groupsCheckEmpty = null
            then expandModifiedName
            else Table.ExpandTableColumn(expandModifiedName, "GroupName", {"name"}, {"GroupName.name"}),

        delFinal = Table.RemoveColumns(expandGroupsName,{"created_user_id", "responsible_user_id", "group_id", "tags", "modified_user_id", "custom_fields"}),
        renameFinal = Table.RenameColumns(delFinal,{{"Tags.1", "Tags"}})
in
    renameFinal
in
getFn
