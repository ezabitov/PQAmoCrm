let
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
    getResponse2
in
    guideConnect
