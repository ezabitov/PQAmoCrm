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

        //Типов задач
        tasktypeRecord = getAccountInfo[task_types],
        tasktypeToTable = Table.FromList(tasktypeRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        tasktypeExpandNames = Table.ExpandRecordColumn(tasktypeToTable, "Column1", {"id", "name", "code"}, {"id", "name", "code"}),
        tasktypeCheckDefault = Table.AddColumn(tasktypeExpandNames, "Пользовательская", each
            try if [id] < 5 then [code] else 0 otherwise [id]),
        tasktypeDelAnother = Table.SelectColumns(tasktypeCheckDefault,{"name", "Пользовательская"}),
        tasktypeRenameId = Table.RenameColumns(tasktypeDelAnother,{{"Пользовательская", "id"}}),
        tasktypeToText = Table.TransformColumnTypes(tasktypeRenameId,{{"id", type text}}),

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
                RelativePath="/private/api/v2/json/tasks/list",
                Query=newAuthQuery
            ])),
        toTable = Record.ToTable(getQuery),
        delOther = Table.SelectColumns(toTable,{"Value"}),
        expand = Table.ExpandRecordColumn(delOther, "Value", {"tasks"}, {"tasks"}),
        expand1 = Table.ExpandListColumn(expand, "tasks"),
    expand2 = Table.ExpandRecordColumn(expand1, "tasks", {"id", "element_id", "element_type", "task_type", "date_create", "created_user_id", "last_modified", "text", "responsible_user_id", "complete_till", "status", "group_id", "account_id", "result"}, {"id", "element_id", "element_type", "task_type", "date_create", "created_user_id", "last_modified", "text", "responsible_user_id", "complete_till", "status", "group_id", "account_id", "result"}),
        //Перевод дат из timestamp
        timestampDateCreate = Table.AddColumn(expand2, "Date_create", each if [date_create] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_create])),
        timestampDateModified = Table.AddColumn(timestampDateCreate, "Last_modified", each if [last_modified] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[last_modified])),
    timestampDate_till = Table.AddColumn(timestampDateModified, "Deadline_Date", each if [complete_till] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[complete_till])),
        removeOldDates = Table.RemoveColumns(timestampDate_till,{"date_create", "last_modified"}),
    removeOldDatesToText = Table.TransformColumnTypes(removeOldDates,{{"created_user_id", type text}, {"responsible_user_id", type text}, {"element_type", type text}, {"group_id", type text}}),


        //merge со справочниками
        mergeWithCreateUserName = Table.NestedJoin(
            removeOldDatesToText,{"created_user_id"},
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
        mergeWithTasktype = Table.NestedJoin(
            mergeWithGroupsName,{"task_type"},
            tasktypeToText,{"id"},
            "TaskType",JoinKind.LeftOuter),
    expandTasktype = Table.ExpandTableColumn(mergeWithTasktype, "TaskType", {"name"}, {"TaskType.name"}),
    //expand
        expandCreaterName = Table.ExpandTableColumn(mergeWithGroupsName, "CreatedUser", {"name"}, {"CreatedUser.name"}),
        expandResponsibleName = Table.ExpandTableColumn(expandCreaterName, "ResponsibleUser", {"name"}, {"ResponsibleUser.name"}),
        expandGroupsName = if groupsCheckEmpty = null
            then expandResponsibleName
            else Table.ExpandTableColumn(expandResponsibleName, "GroupName", {"name"}, {"GroupName.name"}),


    addColumnTypeOfElement = Table.AddColumn(expandGroupsName, "Тип элемента", each if [element_type] = "2" then "Сделка" else if [element_type] = "1" then "Контакт" else if [element_type] = "3" then "Компания" else if [element_type] = "12" then "Покупатель" else "Неизвестно" ),
    addColumnResultText = Table.AddColumn(addColumnTypeOfElement, "Result_Text", each Record.Field([result], "text")),
    replaceErrors = Table.ReplaceErrorValues(addColumnResultText, {{"Result_Text", null}}),
    delFinal = Table.RemoveColumns(replaceErrors,{"created_user_id", "responsible_user_id", "group_id", "task_type", "result", "complete_till", "element_type"})
in
    delFinal
in
getFn
