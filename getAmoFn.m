/*
     Сбор данных из AmoCRM в Excel/Power BI

     Версия 1.1

     1.1
     -- Добавлена автоматическая обработка пустого поля limits
     -- Исправлены баги при обработки contacts

     Создатель: Эльдар Забитов (http://zabitov.ru)
*/

let
getAmoFn = (domen as text, login as text, hash as text, typeOfReport as text, limits as nullable number) =>
let
    //вводные
    limits = if limits = null then 100000 else limits,
    authKey = "?USER_LOGIN="&login&"&USER_HASH="&hash,
    authUrl = "https://"&domen&".amocrm.ru/private/api/auth.php",

    //генерируем массив с данными от 0 до 10к, с шагом в 500 и делаем из него таблицу
    generateList = List.Generate(()=>1, each _ < limits, each _ + 500),
    listToTable = Table.FromList(generateList, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    numberToText = Table.TransformColumnTypes(listToTable,{{"Column1", type text}}),

    //забираем справочники из сведений аккаунта
    getAccountInfo = Json.Document(Web.Contents("https://"&domen&".amocrm.ru/private/api/v2/json/accounts/current"&authKey)),
    getResponse = getAccountInfo[response],
    getResponse2 = getResponse[account],

    //справочник имен пользователей
    usersRecord = getResponse2[users],
    usersToTable = Table.FromList(usersRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    usersExpandNames = Table.ExpandRecordColumn(usersToTable, "Column1", {"id", "name"}, {"id", "name"}),

    //справочник названий статусов
    statusesRecord = getResponse2[leads_statuses],
    statusesToTable = Table.FromList(statusesRecord, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    statusesExpandNames = Table.ExpandRecordColumn(statusesToTable, "Column1", {"id", "name"}, {"id", "name"}),
    statusesChangeType = Table.TransformColumnTypes(statusesExpandNames,{{"id", type text}}),

    //генерим функцию подстановки данных массива в limit_offset, чтоб избежать лимита в 500 записей за раз
    getFn = (limitOffset as text) =>
    let
        url =  "https://"&domen&".amocrm.ru/private/api/v2/json/"&typeOfReport&"/list",
        limits = "&limit_rows=500&limit_offset="&limitOffset,
        getAuth = Xml.Tables(Web.Contents(authUrl&authKey)),
        authTrue = Table.TransformColumnTypes(getAuth,{{"auth", type logical}}),
        getQuery  = Json.Document(Web.Contents(url&authKey&limits))
    in
        getQuery,

    //разбираем то что получили
    getFnToTable = Table.AddColumn(numberToText, "Custom", each getFn([Column1])),
    expandCustom = Table.ExpandRecordColumn(getFnToTable, "Custom", {"response"}, {"response"}),
    expandResponse = Table.ExpandRecordColumn(expandCustom, "response", {"leads", "contacts", "server_time"}, {"leads", "contacts", "server_time"}),
    deleteErrors = Table.RemoveRowsWithErrors(expandResponse, {"leads"}),
    leadOrContact = Table.AddColumn(deleteErrors, "Data", each if [contacts]=null then [leads] else [contacts]),
    deleteOther = Table.SelectColumns(leadOrContact,{"Data"}),
    expandListLeads = Table.ExpandListColumn(deleteOther, "Data"),

    //раскрываем список столбцов в зависимости от типа отчета
    expand1 = if typeOfReport = "leads" then
        Table.ExpandRecordColumn(expandListLeads, "Data", {"id", "name", "date_create", "created_user_id", "last_modified", "account_id", "price", "responsible_user_id", "linked_company_id", "group_id", "pipeline_id", "date_close", "closest_task", "deleted", "status_id", "custom_fields", "main_contact_id", "tags"}, {"id", "name", "date_create", "created_user_id", "last_modified", "account_id", "price", "responsible_user_id", "linked_company_id", "group_id", "pipeline_id", "date_close", "closest_task", "deleted", "status_id", "custom_fields", "main_contact_id", "tags"})
        else
        Table.ExpandRecordColumn(expandListLeads, "Data", {"id", "name", "last_modified", "account_id", "date_create", "created_user_id", "responsible_user_id", "group_id", "closest_task", "linked_company_id", "company_name", "type", "custom_fields", "linked_leads_id", "tags"}, {"id", "name", "last_modified", "account_id", "date_create", "created_user_id", "responsible_user_id", "group_id", "closest_task", "linked_company_id", "company_name", "type", "custom_fields", "linked_leads_id", "tags"}),

    //timestamp to datetime
    timestampDateCreate = Table.AddColumn(expand1, "Date_create", each #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_create])),
    timestampDateModified = if typeOfReport = "leads"
        then Table.AddColumn(timestampDateCreate, "Last_modified", each #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[last_modified]))
        else timestampDateCreate,
    timestampDateClose = if typeOfReport = "leads"
        then Table.AddColumn(timestampDateModified, "Date_close", each #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[date_close]))
        else timestampDateCreate,
    removeOldDates = if typeOfReport = "leads"
        then Table.RemoveColumns(timestampDateClose,{"date_create", "last_modified", "date_close"})
        else Table.RemoveColumns(timestampDateClose,{"date_create"}),

    //генерируем справочник custom_fields
    deleteOther1 = Table.SelectColumns(removeOldDates,{"id", "custom_fields"}),
    expandCustomFields = Table.ExpandListColumn(deleteOther1, "custom_fields"),
    unpivot = Table.UnpivotOtherColumns(expandCustomFields, {"id"}, "Атрибут", "Значение"),
    unpivot1 = Table.UnpivotOtherColumns(unpivot, {"id"}, "Атрибут.1", "Значение.1"),
    filtering = Table.SelectRows(unpivot1, each ([Атрибут.1] = "Значение")),
    expandCustomFiledsName = Table.ExpandRecordColumn(filtering, "Значение.1", {"id", "name", "values"}, {"Значение.1.id", "Значение.1.name", "Значение.1.values"}),
    expandCustomFieldsValues = Table.ExpandListColumn(expandCustomFiledsName, "Значение.1.values"),
    expandCustomFieldsValues1 = Table.ExpandRecordColumn(expandCustomFieldsValues, "Значение.1.values", {"value"}, {"Значение.1.values.value"}),
    deleteOther2 = Table.SelectColumns(expandCustomFieldsValues1,{"Значение.1.values.value", "Значение.1.name", "id"}),
    getOnlyCustomFields = Table.SelectColumns(deleteOther2,{"Значение.1.name"}),
    distinctCustomFields = Table.Distinct(getOnlyCustomFields),
    listOfCustomFields = Table.ToList(distinctCustomFields),
    getCustomFieldsGuide = Table.Pivot(deleteOther2, List.Distinct(deleteOther2[Значение.1.name]), "Значение.1.name", "Значение.1.values.value"),

    //merge справочников и данных нашей таблицы
    mergeWithCustomFields = Table.NestedJoin(removeOldDates,{"id"},getCustomFieldsGuide,{"id"},"NewColumn",JoinKind.LeftOuter),
    mergeWithCreateUserName = Table.NestedJoin(mergeWithCustomFields,{"created_user_id"},usersExpandNames,{"id"},"usersName",JoinKind.LeftOuter),
    mergeWithRsponsibleUserName = Table.NestedJoin(mergeWithCreateUserName,{"responsible_user_id"},usersExpandNames,{"id"},"ResponsibleUserName",JoinKind.LeftOuter),
    mergeWithStatusesName = if typeOfReport = "leads"
        then Table.NestedJoin(mergeWithRsponsibleUserName,{"status_id"},statusesChangeType,{"id"},"statusesName",JoinKind.LeftOuter)
        else mergeWithRsponsibleUserName,
    expandCustomFieldsGuide = Table.ExpandTableColumn(mergeWithStatusesName, "NewColumn", listOfCustomFields, listOfCustomFields),
    changeTypeOfStatusId = Table.TransformColumnTypes(mergeWithCustomFields,{{"status_id", type text}}),
    expandUsersName = Table.ExpandTableColumn(changeTypeOfStatusId, "usersName", {"name"}, {"created_user_name"}),
    expandResponsibleName = Table.ExpandTableColumn(expandUsersName, "ResponsibleUserName", {"name"}, {"responsible_user_name"}),
    expandStatusesName = if typeOfReport = "leads"
        then Table.ExpandTableColumn(expandResponsibleName, "statusesName", {"name"}, {"status_name"})
        else expandResponsibleName,
    deleteOldCustomFields = if typeOfReport = "leads"
        then Table.RemoveColumns(expandStatusesName,{"custom_fields", "responsible_user_id", "created_user_id", "status_id"})
        else Table.RemoveColumns(expandStatusesName,{"custom_fields", "responsible_user_id", "created_user_id"})
in
    deleteOldCustomFields
in
getAmoFn
