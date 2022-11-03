import std/[asyncnet, asyncdispatch]
import strutils
import strformat
import tables
import db_sqlite
import times

let db = open("bedis.db", "", "", "")
db.exec(sql"""
PRAGMA journal_mode=WAL;
create table if not exists bedis
(
    db     INTEGER default 0,
    key    TEXT not null,
    value  TEXT,
    expiry INTEGER,
    constraint bedis_pk
        primary key (db, key)
);

create index bedis_expiry_index
    on bedis (expiry);
""")

proc remove[T](s: var seq[T], ind: int): T {.inline.} =
    result = s[ind]
    s.delete(ind)

type
    bedisObject = ref object of RootObj

    bedisConnectionClosed = object of CatchableError

    bedisNil = ref object of bedisObject

    bedisSimpleString = ref object of bedisObject
        data: string

    bedisError = ref object of bedisObject
        data: string

    bedisInteger = ref object of bedisObject
        data: int

    bedisBulkString = ref object of bedisSimpleString

    bedisArray = ref object of bedisObject
        data: seq[bedisObject]

    bedisClient = ref object of RootObj
        s: AsyncSocket
        currentDatabase: int


template `requireStr`(input: seq[bedisObject]): string =
    if not (input[0] of bedisSimpleString):
        await client.s.send($bedisError(data: "Invalid argument type"))
        return
    input.remove(0).bedisSimpleString.data



proc `$`(o: bedisError): string =
    return &"-{o.data}\c\L"

proc `$`(o: bedisSimpleString): string =
    return &"+{o.data}\c\L"

proc `$`(o: bedisBulkString): string =
    return &"${o.data.len()}\c\L{o.data}\c\L"

proc `$`(o:bedisNil): string =
    return "$-1\c\L"

proc `$`(o:bedisInteger): string =
    return &":{o.data}\c\L"

proc `$`(o: bedisObject): string =
    # this kills me
    if o of bedisError:
        return $o.bedisError
    elif o of bedisBulkString:
        return $o.bedisBulkString
    elif o of bedisSimpleString:
        return $o.bedisSimpleString
    elif o of bedisInteger:
        return $o.bedisInteger
    elif o of bedisArray:
        let data = o.bedisArray.data
        var s = &"*{data.len()}\c\l"
        for dat in data:
            s &= $dat
        return s
    else:
        raise newException(ValueError, "Unhandled bedisObject")
    return ""

proc ok(client: bedisClient) {.async.} =
    await client.s.send(
        $bedisSimpleString(data: "OK")
    )

proc nilResp(client: bedisClient) {.async.} =
    await client.s.send(
        $bedisNil()
    )

proc readObject(client: bedisClient): Future[bedisObject] {.async.} =
    let indChar = await client.s.recv(1)
    case indChar:
        of "*":
            var count = parseInt(await client.s.recvLine())
            var obj = bedisArray(data: newSeq[bedisObject]())

            for num in 1 .. count:
                obj.data.add(await client.readObject())
            return obj
        of "+":
            return bedisSimpleString(
                data: await client.s.recvLine()
            )
        of ":":
            return bedisInteger(
                data: parseInt(await client.s.recvLine())
            )
        of "-":
            return bedisError(
                data: await client.s.recvLine()
            )
        of "$":
            var length = parseInt(await client.s.recvLine())
            if length == -1:
                return bedisNil()
            result = bedisBulkString(
                data: await client.s.recv(length)
            )
            discard client.s.recv(2) # get rid of \r\n
            return
        of "":
            raise newException(bedisConnectionClosed, "Connection closed.")
        else:
            await client.s.send($bedisError(data: "Unknown symbol: " & indChar))


proc set(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    var key = requireStr largs
    var value = requireStr largs

    var expiry = ""
    var get = false
    var x = 0

    while largs.len() > 0:
        let handled = requireStr largs
        case handled:
            of "EX":
                expiry = $(parseInt(requireStr largs) + getTime().toUnix())
            of "EXAT":
                expiry = requireStr largs
            of "NX":
                x = 1
            of "XX":
                x = 2
            of "GET":
                get = true
    var check = db.prepare("SELECT * FROM `bedis` WHERE `db` = ? AND `key` = ? LIMIT 1")
    check.bindParams(client.currentDatabase, key)
    var last = db.getAllRows(check)

    if x > 0:
        if (x == 1 and last.len() == 1) or (x == 2 and last.len() == 0):
            await client.nilResp()
            finalize(check)
            return


    var query = db.prepare("INSERT OR REPLACE INTO `bedis`(`db`, `key`, `value`, `expiry`) VALUES (?,?,?,?)")
    query.bindParam(1, client.currentDatabase)
    query.bindParam(2, key)
    query.bindParam(3, value)
    if expiry != "":
        query.bindParam(4, expiry)
    discard db.tryExec(query)
    finalize(query)
    if get and last.len() > 0:
        await client.s.send(
            $bedisBulkString(data: last[0][2])
        )
    else:
        await client.ok()

proc mset(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]


    while largs.len() > 1:
        var key = requireStr largs
        var value = requireStr largs
        var query = db.prepare("INSERT OR REPLACE INTO `bedis`(`db`, `key`, `value`) VALUES (?,?,?)")
        query.bindParams(client.currentDatabase, key, value)

        discard db.tryExec(query)
        finalize(query)
    await client.ok()

proc get(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    var key = requireStr largs

    var query = db.getAllRows(sql"SELECT * FROM `bedis` WHERE `db` = ? AND `key` = ?", client.currentDatabase, key)
    if query.len() == 0:
        await client.nilResp()
    else:
        await client.s.send($bedisBulkString(data: query[0][2]))

proc mget(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    var resp = bedisArray(
        data: newSeq[bedisObject]()
    )
    while largs.len() > 0:
        let key = requireStr largs
        var query = db.getAllRows(sql"SELECT * FROM `bedis` WHERE `db` = ? AND `key` = ?", client.currentDatabase, key)
        if query.len() == 0:
            resp.data.add(bedisNil())
        else:
            resp.data.add(bedisBulkString(data: query[0][2]))
    await client.s.send(
        $resp
    )



proc delete(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    var deleted = 0
    while largs.len() > 0:
        var arg = requireStr largs
        var query = db.prepare("DELETE FROM `bedis` WHERE `db` = ? AND `key` = ?")
        query.bindParams(client.currentDatabase, arg)
        discard db.tryExec(query)
        if db.getValue(sql"SELECT changes() as chg") == "1":
            inc deleted
        finalize(query)

    await client.s.send(
        $bedisInteger(data: deleted)
    )

proc select(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    var db = requireStr largs
    try:
        client.currentDatabase = parseInt(db)
        await client.ok()
    except ValueError:
        await client.s.send(
            $bedisError(data: "Invalid database")
        )

proc pong(client: bedisClient, args: seq[bedisObject]) {.async.} =
    var largs = args[1..^1]
    if largs.len() == 0:
        await client.s.send(
            $bedisBulkString(
                data: "PONG"
            )
        )
    else:
        await client.s.send(
            $bedisBulkString(
                data: requireStr largs
            )
        )

proc handleCommand(client: bedisClient, arr: bedisArray) {.async.} =
    var data = arr.data
    if data.len() == 0:
        await client.s.send($bedisError(data: "Empty array"))
        return
    if not (arr.data[0] of bedisSimpleString):
        await client.s.send($bedisError(data: "I don't know what to do with that"))
        return

    case data[0].bedisSimpleString.data:
        of "SET":
            await set(client, data)
        of "GET":
            await get(client, data)
        of "DEL":
            await delete(client, data)
        of "SELECT":
            await select(client, data)
        of "MGET":
            await mget(client, data)
        of "MSET":
            await mset(client, data)
        of "PING":
            await pong(client, data)



proc processClient(cli: AsyncSocket) {.async.} =
    var client = bedisClient(
        currentDatabase: 0,
        s: cli
    )
    try:
        while true:
            var obj = await client.readObject()
            if (obj of bedisArray):
                await handleCommand(client, obj.bedisArray)
    except bedisConnectionClosed:
        return


proc cleanup() {.async.} =
    while true:
        db.exec(sql"DELETE FROM `bedis` WHERE `expiry` NOT NULL AND `expiry` < ?", getTime().toUnix())
        await sleepAsync(1000)


proc serve() {.async.} =
    var server = newAsyncSocket()
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(6379))
    server.listen()

    while true:
        asyncCheck processClient(await server.accept())

asyncCheck serve()
asyncCheck cleanup()
runForever()