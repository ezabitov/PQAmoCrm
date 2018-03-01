let
amoFn = (method as text, domen as text, login as text, hash as text, limits as nullable number) =>
    let
        authQuery =
            [
                USER_LOGIN=login,
                USER_HASH=hash
                ],
        url = "https://"&domen&".amocrm.ru",
        limit = if limits = null then 20000 else limits,

        githubFn = (function as text) =>
            let
                sourceFn = Expression.Evaluate(
                    Text.FromBinary(
                        Binary.Buffer(
                            Web.Contents("https://raw.githubusercontent.com/ezabitov/PQAmoCrm/master/get"&function&".m")
                        )
                    ), #shared)
            in
                sourceFn,

        generateList = List.Generate(()=>0, each _ < limit, each _ + 500),
        listToTable = Table.FromList(generateList, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        numberToText = Table.TransformColumnTypes(listToTable,{{"Column1", type text}}),

        getMethod = githubFn(Text.Proper(method)),

        getFnToTable = Table.AddColumn(numberToText, Text.Proper(method), each getMethod([Column1], url, authQuery)),
        removeErrors = Table.RemoveRowsWithErrors(getFnToTable, {Text.Proper(method)}),
        removeColumn = Table.RemoveColumns(removeErrors,{"Column1"})
    in
        removeColumn
in
    amoFn
