let
guideConnect = (domen as text, login as text, hash as text, limits as number) =>
    let
        authQuery =
            [
                USER_LOGIN=login,
                USER_HASH=hash
                ],
        url = "https://"&domen&".amocrm.ru",

        generateList = List.Generate(()=>0, each _ < limits, each _ + 500),
        listToTable = Table.FromList(generateList, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        numberToText = Table.TransformColumnTypes(listToTable,{{"Column1", type text}}),

        getFnToTable = Table.AddColumn(numberToText, "getLeads", each getLeads([Column1], url, authQuery))
    in
        getFnToTable
in
    guideConnect
