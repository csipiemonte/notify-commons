/**
 * Class used to create queries
 */
var filtri = {
    "eq":"$1 = '$2'",
    "ne":"$1 != '$2'",
    "lt":"$1 < '$2'",
    "lte":"$1 <= '$2'",
    "gt":"$1 > '$2'",
    "gte":"$1 >= '$2'",
    "in":"$1 IN($2)",
    "nin":"$1 NOT IN($2)",
    "c":"COALESCE($1,'') LIKE '%$2%'",
    "ci":"COALESCE($1,'') ILIKE '%$2%'",
    "cai":"array_to_string($1, ',') ILIKE '%$2%'",
    "nc":"COALESCE($1,'') NOT LIKE '%$2%'",
    "nci":"COALESCE($1,'') NOT ILIKE '%$2%'",
    "s":"$1 LIKE '$2%'",
    "si":"$1 ILIKE '$2%'",
    "e":"$1 LIKE '%$2'",
    "ei":"$1 ILIKE '%$2'",
    "null": "(($1 is null and $2) or ($1 is not null and not $2))",
    "match": ($1, $2) => matchArray($1, $2),
    "not_match": ($1, $2) => notMatchArray($1, $2)
  }
  
function matchArray(field, values) {
        var whereClause = "";
        var conditions = values.split(" ");
        var openBracket = true;
        var closeBracket = false;
        var isNot = false;
        var isAnd = false;

        for (var i in conditions) {
            var condition = conditions[i].trim();

            var not = "";
            if (condition.substring(0, 1) === "-") {
                if (closeBracket === true) {
                    whereClause += ") ";
                    closeBracket = false;
                }
                isAnd = true;
                openBracket = true;
                isNot = true;
                condition = condition.substring(1);
            }

            if (condition.substring(0, 1) === "+") {
                if (closeBracket === true) {
                    whereClause += ") ";
                    closeBracket = false;
                }
                isAnd = true;
                openBracket = true;
                isNot = false;
                condition = condition.substring(1);
            }

            if (condition.length > 0) {
                if (whereClause.length > 0) {
                    if (isAnd === true) {
                        whereClause += "AND ";
                    } else {
                        whereClause += "OR ";
                    }
                }
                if (openBracket === true) {
                    whereClause += "( ";
                    openBracket = false;
                    closeBracket = true;
                }
                if (isNot === true) {
                    whereClause += "NOT ";
                }
                whereClause += field + " @> array['" + condition + "'] ";
            }
        }
        if (closeBracket === true) {
            whereClause += ")";
        }

        return "(" + whereClause + ")";
}

function notMatchArray(field, values) {
    return "NOT " + matchArray(field, values);
}

function filter(f) {
    if (!f) return this;
    if (!(typeof(f) === 'object'))
        try {
            f = JSON.parse(f);
        } catch (e) {
            throw ({message: "filter must be an object or a string that represents it", status: 400});
        }
    this.sql += " where 1=1";
    for (var field in f) this.sql += clause(field, f[field]);


    return this;
}

/*function clause(field, expression) {
    var result = "";
    for (var operator in expression) {
        console.log("operator:", operator);
        console.log("expression:", expression);
        var value = expression[operator];

        if (value instanceof Array) {
            value = [value.map(e => {
                e.replace(/'/g, "''");
                return "'" + e + "'";
            }).join()];
        } else if(value instanceof String) {
            value = [value.replace(/'/g, "''")];
        }else if (value instanceof Object) {
            if (!value.values) throw ({
                message: "if the value is an object, it should be contains a key named values",
                status: 400
            });
            value = (value.values.map((e) => {
                return e.replace(/'/g, "''");
                //return "'" + e + "'";
            }));
        } else {
            value=[value];
        }

        value = value.map(keyValue => {
            if (keyValue.match(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)) {
                if (!field.toLowerCase().startsWith("unix_timestamp")) field = "unix_timestamp(" + field + ")";
                return new Date(keyValue).getTime() / 1000;
            }
            return keyValue;
        });

        //console.log("field: " + field);
        //console.log("value: " + value);
        value.forEach(theValue => result += " and " + filtri[operator].replace(/\$1/g, field).replace(/\$2/g, theValue));

    }
    return result;
}*/
function clause(field, expression) {
    var result = "";
    for (var operator in expression) {
        var value = expression[operator];
        value = "string" === typeof value ? value.replace(/'/g, "''") : value;
        if (value instanceof Array) value = value.map((e) => {
            e.replace(/'/g, "''");
            return "'" + e + "'"
        }).join();

        /*if (value.match(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)) {
            value = new Date(value).getTime() / 1000;
            //if (!field.toLowerCase().startsWith("unix_timestamp")) field = "unix_timestamp(" + field + ")";
            //value = "FROM_UNIXTIME(" + value + ")";
        }*/

        //console.log("field: " + field);
        //console.log("value: " + value);

        result += " and " +  (typeof filtri[operator] === 'string' ? filtri[operator].replace(/\$1/g, field).replace(/\$2/g, value) : filtri[operator](field,value));
    }
    return result;
}


function fields(f) {
    if (!f) return this;
    if (!typeof(f) === 'string') throw ({message: "fields must be a string", status: 400});
    var splitted = f.split(",");

//    if (splitted.length != splitted.filter(e => e.trim().toUpperCase().match(/^[A-Z_][A-Z0-9_]+$|^\*$/g)).length)
    if (splitted.length !== splitted.filter(e => e.toUpperCase().match(/(^[A-Z_\*][A-Z0-9_\.\*\s]+$|^COUNT \(\*\) AS COUNT$)/g)).length)
        throw {message: "fields not valid", status: 400};

    this.sql = this.sql.replace("*", f);
    return this;
}

function distinct() {

    this.sql = this.sql.replace("select", "select distinct");
    return this;
}

function page(limit, offset) {
    if (!limit) limit = 10;
    if (isNaN(limit) || limit <= 0) throw ({message: "limit not valid", status: 400});
    if (!offset || isNaN(offset) || offset < 0) offset = 0;
    // if (isNaN(offset) || offset < 0) throw ({message: "offset not valid", status: 400});

    this.sql += ' limit ' + limit + ' offset ' + offset;
    return this;
}

function sort(s) {

    if (!s) return this;
    var splittedSort = s.split(",");
    if (splittedSort.length !== splittedSort.filter(e => e.trim().toUpperCase().match(/^[A-Z_+-][A-Z_+-]+$|^\*$/g)).length)
        throw ({message: "sort fields not valid", status: 400});

    this.sql += " ORDER BY " +
        splittedSort
            .map(e => e.substring(1) + " " + (e.charAt(0) === '+' ? 'ASC' : 'DESC'))
            .join(",");
    return this;
}

// function count() {
//     this.sql = "select count(1) as count from (" + this.sql + ") t";
//     return this;
// }
function count() {
    this.fields("count (*) as count");
    return this;
}

function table(t) {
    if (!t) throw ({message: "table is null", status: 400});
    if (!typeof(t) === 'string') throw ({message: "table must be a string", status: 400});
    //if (!t.toUpperCase().match(/^[A-Z_]+[A-Z0-9_]*$/)) throw ({message: "table <" + t + "> not valid", status: 400});
    this.sql += t;
    return this;
}

/**
 *
 * @param direction LEFT,RIGHT,INNER or undefined
 */
function join(direction) {
    this.sql += (direction ? " " + direction : "") + " join ";
    return this;
}

/**
 *
 * @param clauses string that contains the clauses
 */
function on(clauses) {
    this.sql += " on " + clauses + " ";
    return this;
}

function select(selectTable) {
    let sql = "select * from ";
    if(selectTable) sql = sql.replace("*",selectTable);
    this.sql = sql;
    return this;
}

function groupBy(group_by) {
    this.sql += " group by " + group_by;
    return this;
}

function deleteFrom() {
    this.sql = "delete from ";
    return this;
}

function insert() {
    this.sql = "insert into ";
    return this;
}

function values(insFields, insValues) {
    if ((typeof(insFields) === 'object')) {
        try {
            var json = insFields;
            insFields = Object.keys(json).join(",");
            insValues = Object.keys(json).map(e => json[e]);

        } catch (e) {
            throw ({message: "insFields must be an object or a string that represents it", status: 400});
        }
    }


    insValues = insValues.map(e => {
        if (e !== undefined && e !== null) {
            return Array.isArray(e)? "ARRAY"+JSON.stringify(e).replace(/"/g,"'") : "'" + postgres_escape(e) + "'";
            //return "'" + (typeof e === "string" ? e.replace(/'/g, "''") : e) + "'";
        } else {
          console.log(e);
            return 'NULL';
        }
    });
    this.sql += "(" + insFields + ") values(" + insValues.join(",") + ")";
    return this;
}

/**
 * create a multi value insert sql statement
 * @param insFields
 * @param insValues array of array, an array that contains an array of values
 * @returns {multipleValues}
 */
function multipleValues(insFields, insValues) {

    //console.log(insValues);
    this.sql += "( " + insFields + ") values";
    insValues = insValues.map(e => e.map(e1 => "'" + (typeof e1 === "string" ? e1.replace(/'/g, "''") : e1) + "'"));
    insValues.forEach(e => this.sql += "(" + e.join(",") + "),");

    // remove last ","
    this.sql = this.sql.substring(0, this.sql.length - 1);

    return this;
}

/**
 * Get information from column such as the possible value of an enum
 * @param dbName
 * @param tableName
 * @param columnName
 */
function columnInformation(dbName, tableName, columnName) {
    this.sql = "SELECT COLUMN_TYPE AS type\n" +
        "FROM information_schema.COLUMNS\n" +
        "WHERE TABLE_SCHEMA='" + dbName + "' " +
        "    AND TABLE_NAME='" + tableName + "' " +
        "    AND COLUMN_NAME='" + columnName + "' ";
    return this;

}

function update() {
    this.sql = "update ";
    return this;
}

function set(columns, values) {

    if (!Array.isArray(columns) && typeof(columns) === 'object') {
        try {
            var json = columns;
            columns = Object.keys(json);
            values = Object.keys(json).map(e => json[e]);
        } catch (e) {
            throw ({message: "columns must be an object or a string that represents it or an array", status: 400});
        }
    }


    if (!columns) throw ({status: 400, message: "columns not setted"});
    if (columns.length !== values.length) throw ({status: 400, message: "columns and values must have same size"});

    values = values.map(e => typeof e === "string"? "'" + postgres_escape(e) + "'" : e).map(e => Array.isArray(e)? "ARRAY"+JSON.stringify(e).replace(/"/g,"'") : e);
    this.sql += " set";
    columns.forEach((col, i) => this.sql += " " + col + "=" + values[i] + ",");

    // remove last ","
    this.sql = this.sql.substring(0, this.sql.length - 1);

    return this;

}


function postgres_escape(s) {
    let str = typeof s === "object"? JSON.stringify(s) : s + "";
    return str.replace(/'/g, "''");
/*    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
            case "%":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });
*/
}


var q = {
    insert: insert,
    values: values,
    select: select,
    join: join,
    on: on,
    delete: deleteFrom,
    table: table,
    fields: fields,
    filter: filter,
    page: page,
    sort: sort,
    count: count,
    distinct: distinct,
    groupBy: groupBy,
    multipleValues: multipleValues,
    columnInformation: columnInformation,
    update: update,
    set: set
};


module.exports = q;
