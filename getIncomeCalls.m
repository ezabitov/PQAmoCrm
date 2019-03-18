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

        //пайплайны
        pipelinesRecord = getAccountInfo[pipelines],
        pipelinesToTable = Record.ToTable(pipelinesRecord),
        pipelinesCheckEmpty = Table.First(pipelinesToTable),
        pipelinesDelAnother = Table.SelectColumns(pipelinesToTable,{"Value"}),
        pipelinesExpandNames = Table.ExpandRecordColumn(pipelinesDelAnother, "Value", {"id", "name"}, {"id", "name"}),
        pipelinesChangeType = Table.TransformColumnTypes(pipelinesExpandNames,{{"id", type text}}),

        //Названий групп
        groupsRecord = getAccountInfo[groups],
        groupsToTable = Table.FromList(groupsRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        groupsCheckEmpty = Table.First(groupsToTable),
        groupsExpandNames = Table.ExpandRecordColumn(groupsToTable, "Column1", {"id", "name"}, {"id", "name"}),
        groupsChangeType = Table.TransformColumnTypes(groupsExpandNames,{{"id", type text}}),

        //Типов задач
        notetypesRecord = getAccountInfo[note_types],
        notetypesToTable = Table.FromList(notetypesRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        notetypesExpandNames = Table.ExpandRecordColumn(notetypesToTable, "Column1", {"id", "name", "code"}, {"id", "name", "code"}),
        notetypesCheckDefault = Table.AddColumn(notetypesExpandNames, "Пользовательская", each
            if [name] = "" then [code] else [name]),
        notetypesDelAnother = Table.SelectColumns(notetypesCheckDefault,{"id", "Пользовательская"}),
        notetypesRename = Table.RenameColumns(notetypesDelAnother,{{"Пользовательская", "name"}}),
        notetypesToText = Table.TransformColumnTypes(notetypesRename,{{"id", type text}}),

/*
-------------------------------------
-------------Справочники-------------
-------------------------------------
*/


        newAuthQuery = Record.Combine({
            authQuery,
            [limit_rows ="500"],
            [limit_offset=limits],
            [type="lead"]}),

        getQuery  = Json.Document(Web.Contents(url,
            [
                RelativePath="/private/api/v2/json/notes/list",
                Query=newAuthQuery
            ])),
        toTable = Record.ToTable(getQuery),
        delOther = Table.SelectColumns(toTable,{"Value"}),
        expand = Table.ExpandRecordColumn(delOther, "Value", {"notes"}, {"notes"}),
        expand1 = Table.ExpandListColumn(expand, "notes"),
        expand2 = Table.ExpandRecordColumn(expand1, "notes", {"id", "element_id", "element_type", "note_type", "date_create", "created_user_id", "last_modified", "text", "responsible_user_id", "account_id", "ATTACHEMENT", "group_id", "editable"}, {"id", "element_id", "element_type", "note_type", "date_create", "created_user_id", "last_modified", "text", "responsible_user_id", "account_id", "ATTACHEMENT", "group_id", "editable"}),


        //Перевод дат из timestamp
        timestampDateCreate = Table.AddColumn(expand2, "Date_create", each if [date_create] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_create])),
        timestampDateModified = Table.AddColumn(timestampDateCreate, "Last_modified", each if [last_modified] = 0 then null else #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[last_modified])),
        removeOldDates = Table.RemoveColumns(timestampDateModified,{"date_create", "last_modified"}),
        removeOldDatesToText = Table.TransformColumnTypes(removeOldDates,{{"created_user_id", type text}, {"responsible_user_id", type text}, {"element_type", type text}, {"note_type", type text}, {"group_id", type text}}),


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
        mergeWithNotetype = Table.NestedJoin(
            mergeWithGroupsName,{"note_type"},
            notetypesToText,{"id"},
            "NoteType",JoinKind.LeftOuter),

        //expand
        expandCreaterName = Table.ExpandTableColumn(mergeWithNotetype, "CreatedUser", {"name"}, {"CreatedUser.name"}),
        expandResponsibleName = Table.ExpandTableColumn(expandCreaterName, "ResponsibleUser", {"name"}, {"ResponsibleUser.name"}),
        expandGroupsName = if groupsCheckEmpty = null
            then expandResponsibleName
            else Table.ExpandTableColumn(expandResponsibleName, "GroupName", {"name"}, {"GroupName.name"}),
            
        addColumnTypeOfElement = Table.AddColumn(expandGroupsName, "Тип элемента", each
            if [element_type] = "2" then "Сделка" else
            if [element_type] = "1" then "Контакт" else
            if [element_type] = "3" then "Компания" else
            if [element_type] = "12" then "Покупатель" else "Неизвестно" ),


        expandNoteType = Table.ExpandTableColumn(addColumnTypeOfElement, "NoteType", {"name"}, {"NoteType.name"}),
   delFinal = Table.RemoveColumns(expandNoteType,{"created_user_id", "responsible_user_id", "group_id", "note_type", "element_type"}),
    #"Разделить столбец по разделителю" = Table.SplitColumn(delFinal, "text", Splitter.SplitTextByDelimiter(""",""", QuoteStyle.None), {"text.1", "text.2", "text.3", "text.4"}),
    #"Разделить столбец по разделителю1" = Table.SplitColumn(#"Разделить столбец по разделителю", "text.2", Splitter.SplitTextByDelimiter(""":""", QuoteStyle.None), {"text.2.1", "STATUS_OLD"}),
    #"Разделить столбец по разделителю2" = Table.SplitColumn(#"Разделить столбец по разделителю1", "text.1", Splitter.SplitTextByDelimiter(""":""", QuoteStyle.None), {"text.1.1", "STATUS_NEW"}),
    #"Разделить столбец по разделителю3" = Table.SplitColumn(#"Разделить столбец по разделителю2", "text.3", Splitter.SplitTextByDelimiter(""":""", QuoteStyle.None), {"text.3.1", "text.3.2"}),
    #"Разделить столбец по разделителю4" = Table.SplitColumn(#"Разделить столбец по разделителю3", "text.4", Splitter.SplitTextByDelimiter(",""", QuoteStyle.None), {"text.4.1", "text.4.2", "text.4.3"}),
    #"Разделить столбец по разделителю5" = Table.SplitColumn(#"Разделить столбец по разделителю4", "text.4.3", Splitter.SplitTextByDelimiter(""":", QuoteStyle.None), {"text.4.3.1", "LOSS_REASON_ID"}),
    #"Замененное значение" = Table.ReplaceValue(#"Разделить столбец по разделителю5","}","",Replacer.ReplaceText,{"LOSS_REASON_ID"}),
    #"Разделить столбец по разделителю6" = Table.SplitColumn(#"Замененное значение", "text.4.2", Splitter.SplitTextByDelimiter(""":", QuoteStyle.None), {"text.4.2.1", "PIPELINE_ID_NEW"}),
    #"Разделить столбец по разделителю7" = Table.SplitColumn(#"Разделить столбец по разделителю6", "text.4.1", Splitter.SplitTextByDelimiter(""":", QuoteStyle.None), {"text.4.1.1", "PIPELINE_ID_OLD"}),
    #"Добавлен пользовательский объект" = Table.AddColumn(#"Разделить столбец по разделителю7", "Text", each if [NoteType.name] = "DEAL_STATUS_CHANGED" then [text.3.2] else [text.1.1]),
    finaldel1 = Table.RemoveColumns(#"Добавлен пользовательский объект",{"text.1.1", "text.2.1", "text.3.1", "text.3.2", "text.4.1.1", "text.4.2.1", "text.4.3.1"}),
        mergeWithStatusNew = Table.NestedJoin(
           finaldel1,{"STATUS_NEW"},
             statusesChangeType,{"id"},
            "NewStatus",JoinKind.LeftOuter),

        mergeWithStatusOld = Table.NestedJoin(
           mergeWithStatusNew,{"STATUS_OLD"},
             statusesChangeType,{"id"},
            "OldStatus",JoinKind.LeftOuter),

        mergeWithOldPipeline = Table.NestedJoin(
           mergeWithStatusOld,{"PIPELINE_ID_OLD"},
             pipelinesChangeType,{"id"},
            "OldPipeline",JoinKind.LeftOuter),

        mergeWithNewPipeline = Table.NestedJoin(
           mergeWithOldPipeline,{"PIPELINE_ID_NEW"},
             pipelinesChangeType,{"id"},
            "NewPipeline",JoinKind.LeftOuter),
    #"Развернутый элемент NewStatus" = Table.ExpandTableColumn(mergeWithNewPipeline, "NewStatus", {"name"}, {"NewStatus.name"}),
    #"Развернутый элемент OldStatus" = Table.ExpandTableColumn(#"Развернутый элемент NewStatus", "OldStatus", {"name"}, {"OldStatus.name"}),
    #"Развернутый элемент OldPipeline" = Table.ExpandTableColumn(#"Развернутый элемент OldStatus", "OldPipeline", {"name"}, {"OldPipeline.name"}),
    #"Развернутый элемент NewPipeline" = Table.ExpandTableColumn(#"Развернутый элемент OldPipeline", "NewPipeline", {"name"}, {"NewPipeline.name"}),
    finaldel2 = Table.RemoveColumns(#"Развернутый элемент NewPipeline",{"STATUS_NEW", "STATUS_OLD", "PIPELINE_ID_OLD", "PIPELINE_ID_NEW"})
in
    finaldel2
in
getFn
