const Connector = require('loopback-connector').Connector;
const {ParameterizedSQL} =  require('loopback-connector');
const odataClient = require('odata-client');
const debug = require('debug')('loopback:connector:odata');
const util = require('util');
const assert = require('assert');
const moment = require('moment');

const NAME = 'odata';

// The generic placeholder
const PLACEHOLDER = ParameterizedSQL.PLACEHOLDER;

const g = require('strong-globalize')();

const dateFormat = 'YYYY-MM-DDTHH:mm:ss';
/**
 * Initialize the Odata connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!odataClient) {
    return;
  }

  dataSource.driver = odataClient;
  const odataSettings = dataSource.settings || {};
  const connector = new OdataDB(odataClient, odataSettings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;

  dataSource.connector.dataSource.log = console.log;

  if (callback) {
    dataSource.connecting = true;
    dataSource.connector.connect(callback);
    // process.nextTick(callback);
  }
};

/**
* Constructor for Odata connector
* @param {Object} settings The settings object
* @param {DataSource} dataSource The data source
* instance
* @constructor
*/
function OdataDB(odata, odataSettings) {
  if (!(this instanceof OdataDB)) {
    return new OdataDB(odata, odataSettings);
  }
  this.constructor.super_.call(this, 'odata', odataSettings);
  this.name = NAME;
  this.settings = odataSettings;
  this.headers = odataSettings.headers || {};
  if (this.settings.username && this.settings.password) {
    const {username, password} = this.settings;
    const auth = 'Basic ' + new Buffer(`${username}:${password}`).toString('base64');
    this.headers['Authorization'] = auth;
  }

  this.odata = odata;
  this.debug = odataSettings.debug;

  if (this.debug) {
    debug('Settings %j', odataSettings);
  }
};

util.inherits(OdataDB, Connector);

/**
* Connect to the Database
* @param callback
*/
OdataDB.prototype.connect = function(callback) {
  var self = this;
  const url = self.settings.url;
  var err = null;
  if (self.client === undefined) {
    self.client = self.odata({
      service: url,
      format: 'json',
    });
    self.dataSource.connected = true;
    callback(err, self.client);
  } else {
    callback(err, self.client);
  }
};

/**
 * The following _abstract_ methods have to be implemented by connectors that
 * extend from SQLConnector to reuse the base implementations of CRUD methods
 * from SQLConnector
 */

/**
 * Converts a model property value into the form required by the
 * database column. The result should be one of following forms:
 *
 * - {sql: "point(?,?)", params:[10,20]}
 * - {sql: "'John'", params: []}
 * - "John"
 *
 * @param {Object} propertyDef Model property definition
 * @param {*} value Model property value
 * @param {Boolean} noEscape for Insert and Updates - no escape values
 * @returns {ParameterizedSQL|*} Database column value.
 *
 */
OdataDB.prototype.toColumnValue = function(propertyDef, value, noEscape) {
  if (propertyDef[NAME] && propertyDef[NAME].dataType) {
    switch (propertyDef[NAME].dataType) {
      case 'table': return Array.isArray(value) ? 
      value.map( v => this.buildFields(propertyDef.type[0].name,v,noEscape).data) : 
      noEscape ? value : `'${value}'`;
      case 'guid': return noEscape ? value : `guid'${value}'`;
      case 'enum':
        if (propertyDef[NAME].values) {
          const {values} = propertyDef[NAME];
          for (const property in values) {
            if (values[property] === value)
              return noEscape ? property : `'${property}'`;
          }
        }
        return noEscape ? value : `'${value}'`;
      default: return noEscape ? value : `'${value}'`;
    }
  };
  switch (propertyDef.type.name) {
    case 'Date':  return noEscape ? value : `datetime'${moment(value).format(dateFormat)}'`;
    case 'Boolean': return value;
    case 'Number': return value;
    default: return noEscape ? value :`'${value}'`;
  }
  // throw new Error(g.f('{{toColumnValue()}} must be implemented by the connector'));
};

/**
 * Return the database name of the property of the model if it exists.
 * Otherwise return the property name.
 * Some connectors allow the column/field name to be customized
 * at the model property definition level as `column`,
 * `columnName`, or `field`. For example,
 *
 * ```json
 * "name": {
 *   "type": "string",
 *   "mysql": {
 *     "column": "NAME"
 *   }
 * }
 * ```
 * @param {String} model The target model name
 * @param {String} prop The property name
 *
 * @returns {String} The database mapping name of the property of the model if it exists
 */
OdataDB.prototype.getPropertyDbName = OdataDB.prototype.column =
function(model, property) {
  const prop = this.getPropertyDefinition(model, property);
  let mappingName;
  if (prop && prop[this.name]) {
    mappingName = prop[this.name].column || prop[this.name].columnName ||
    prop[this.name].field || prop[this.name].fieldName;
    if (mappingName) {
      // Explicit column name, return as-is
      return mappingName;
    }
  }

  // Check if name attribute provided for column name
  if (prop && prop.name) {
    return prop.name;
  }
  mappingName = property;
  if (typeof this.dbName === 'function') {
    mappingName = this.dbName(mappingName);
  }
  return mappingName;
};
/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} where An object for the where conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
OdataDB.prototype.buildWhere = function(model, where) {
  const whereClause = this._buildWhere(model, where);
/*   if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  } */
  return whereClause;
};
/**
 * @private
 * @param model
 * @param where
 * @returns {ParameterizedSQL}
 */
OdataDB.prototype._buildWhere = function(model, where, parentColumn) {
  let columnValue, sqlExp;
  if (!where) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const self = this;
  const props = self.getModelDefinition(model).properties;

  const whereStmts = [];
  for (const key in where) {
    const stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      const branches = [];
      let branchParams = [];
      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (let i = 0, n = clauses.length; i < n; i++) {
          const stmtForClause = self._buildWhere(model, clauses[i]);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: `( ${branches.join(' ' + key + ' ')} )`,
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    const p = props[key];
    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }
    if(p.odata.url) {
      // skip url parameter
      debug('Skipping url property %s for model %s', key, model);
      continue;
    }
    // eslint-disable one-var
    let expression = where[key];
    const columnName = (parentColumn) ? `${parentColumn}/${self.columnEscaped(model, key)}` : self.columnEscaped(model, key);
    // eslint-enable one-var
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' eq null');
    } else if (expression && expression.constructor === Object) {
      const operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (let j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j]));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          const v1 = columnValue[0] === undefined ? null : columnValue[0];
          const v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression);
      }
      if (this.isOperatorChildProperty(p, operator)) {
        // Проверить что оператор (operator) это поле вложенной модели,  если так, то запустить _buildWhere
        // рекурсивно для этой модели и объединить через /

        let childModelName;
        if (Array.isArray(p.type)) {
          childModelName = p.type[0].name;
        } else {
          childModelName = p.type.name;
        }
        sqlExp = self._buildWhere(childModelName, where[key], columnName);
      } else {
        sqlExp = self.buildExpression(columnName, operator, columnValue, p);
      };
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      if(p.odata.objectType) continue; //skip if this is type field
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' eq null');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          stmt.merge(columnName + ' eq ').merge(columnValue);
        } else {
          ///!!!!!!!!!!!!!!! trick for simple types search
          if(p.odata.cast) {
            const typeProp = props[p.odata.cast];
            const type = this.toColumnValue(typeProp, where[p.odata.cast],true).split('.')[1]
            columnValue = `cast(${columnValue}, '${type}')` //sorry, I have no  !!!! FIX ME DUMB 
          } 
          stmt.merge({
            sql: `${columnName} eq ?`,
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  let params = [];
  const sqls = [];
  for (let k = 0, s = whereStmts.length; k < s; k++) {
    if (!whereStmts[k].sql) continue;
    sqls.push(whereStmts[k].sql);
    params = params.concat(whereStmts[k].params);
  }
  const whereStmt = new ParameterizedSQL({
    sql: sqls.join(' and '),
    params: params,
  });
  return whereStmt;
};
/**
 * Check if the operator is a child property field
 * Returns true or false
 * @param {Object} p The field object
 * @param {String} operator The operator name
 */
OdataDB.prototype.isOperatorChildProperty = function(p, operator) {
  let property;
  if (Array.isArray(p.type)) {
    property = this.getModelDefinition(p.type[0].name);
  } else {
    property = this.getModelDefinition(p.type.name);
  }
  return property && property.properties[operator] ? true : false;
};

/**
 * Get the table name for the given model. The table name can be customized
 * at model settings as `table` or `tableName`. For example,
 *
 * ```json
 * "Customer": {
 *   "name": "Customer",
 *   "mysql": {
 *     "table": "CUSTOMER"
 *   }
 * }
 * ```
 *
 * Returns the table name (String).
 * @param {String} model The model name
 */
OdataDB.prototype.table = function(model) {
  const dbMeta = this.getConnectorSpecificSettings(model);
  let tableName;
  if (dbMeta) {
    tableName = dbMeta.table || dbMeta.tableName;
    if (tableName) {
      // Explicit table name, return as-is
      return tableName;
    }
  }
  tableName = model;
  if (typeof this.dbName === 'function') {
    tableName = this.dbName(tableName);
  }
  return tableName;
};
/**
 * Get the escaped column name for a given model property
 * @param {String} property The property name
 * @returns {String} The escaped column name
 */
OdataDB.prototype.escapeName = function(property) {
  return property;
};
/**
 * Get the escaped table name
 * @param {String} model The model name
 * @returns {String} the escaped table name
 */
OdataDB.prototype.tableEscaped = function(model) {
  return this.escapeName(this.table(model));
};

/**
 * Get the escaped column name for a given model property
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The escaped column name
 */
OdataDB.prototype.columnEscaped = function(model, property) {
  return this.escapeName(this.column(model, property));
};
/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @returns {string} Comma separated string of escaped column names
 */
OdataDB.prototype.buildColumnNames = function(model, filter) {
  const fieldsFilter = filter && filter.fields;
  const cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return '*';
  }
  const self = this;
  let keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    const included = [];
    const excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        const index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  const names = keys.map(function(c) {
    return self.columnEscaped(model, c);
  });
  return names;
};
/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @param {object} query The query object
 * @returns {string} Comma separated string of escaped column names
 */
OdataDB.prototype.applyColumnNames = function(model, filter, query) {
  const fieldsFilter = filter && filter.fields;
  const cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return '*';
  }
  const self = this;
  let keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    const included = [];
    const excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        const index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  // Applying expand
  const expand = keys
    .filter(k => cols[k][NAME] && cols[k][NAME].field && cols[k][NAME].dataType === 'object'); 

  const names = keys
    .filter(k => !cols[k][NAME].url) // not an url parameter
    .map(function(c) {
    return self.columnEscaped(model, c);
  });
  if (expand.length) {
    query.expand(...expand.map(k => cols[k][NAME].field));
    expand
      .map(k => this.buildColumnNames(cols[k].type.name, filter && filter.include && filter.include[k])
        .map(c => `${self.columnEscaped(model, k)}/${c}`)
        .forEach(name => names.push(name))
      );
    // names.push(...expand);
  }

  return query.select(names);
};
/**
 * Apply the ORDER BY clause
 * @param {string} model Model name
 * @param {object} object Query object
 * @param {string[]} order An array of sorting criteria
 * @returns {string} The ORDER BY clause
 */
OdataDB.prototype.applyOrderBy = function(model, query, order) {
  if (!order) {
    return query;
  }
  const self = this;
  if (typeof order === 'string') {
    order = [order];
  }
  const clauses = [];
  for (let i = 0, n = order.length; i < n; i++) {
    const t = order[i].split(/[\s,]+/);
    if (t.length === 1) {
      clauses.push([self.columnEscaped(model, order[i])]);
    } else {
      clauses.push([self.columnEscaped(model, t[0]), t[1]]);
    }
  }
  return query.orderby(...clauses);
};


/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @param {Boolean} count Just count objects
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: ...}
 */
OdataDB.prototype.buildSelect = function(model, filter, options, count) {
  if (!filter.order) {
    const idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }
  let url = this.tableEscaped(model);
  const urlRegexp = new RegExp(/\$\{([a-zA-Z0-9_.-]*)\}/i); //Only one param in url
  let resource;
  const urlParams = url.match(urlRegexp);
  if(urlParams) {
    if(filter && filter.where &&  filter.where[urlParams[1]]){     
      url = url.replace(urlRegexp, (m,p1) => this.propToColumnValue(model, p1, filter.where[p1]));
    } else {
      throw new Error(`${urlParams[1]} is required in where clause`);
    }
  } 
  resource = encodeURI(url);
  // const selectArray = this.buildColumnNames(model, filter);
   
  if(count) resource = `${resource}/$count`;

  let query = this.odata({service: this.settings.url,
    format: 'json',
  });

  query.resource(resource);
  query = this.applyColumnNames(model, filter, query);

// build url query and select fields
  if (filter) {
    if (filter.where) {
      const whereStmt = this.buildWhere(model, filter.where);
      const odataFilter = this.parameterize(whereStmt).sql;
      query.filter(odataFilter);
    }

    if (filter.order) {
      query = this.applyOrderBy(model, query, filter.order);
      // put order here
    }

    if (filter.limit || filter.skip || filter.offset) {
      query = this.applyPagination(
        model, query, filter,
      );
      // put pagination here
    }
  }
  return query;
};
/**
 * Replace `?` with connector specific placeholders. For example,
 *
 * ```
 * {sql: 'SELECT * FROM CUSTOMER WHERE NAME=?', params: ['John']}
 * ==>
 * {sql: 'SELECT * FROM CUSTOMER WHERE NAME=:1', params: ['John']}
 * ```
 * *LIMITATION*: We don't handle the ? inside escaped values, for example,
 * `SELECT * FROM CUSTOMER WHERE NAME='J?hn'` will not be parameterized
 * correctly.
 *
 * @param {ParameterizedSQL|Object} ps Parameterized SQL
 * @returns {ParameterizedSQL} Parameterized SQL with the connector specific
 * placeholders
 */
OdataDB.prototype.parameterize = function(ps) {
  ps = new ParameterizedSQL(ps);

  // The value is parameterized, for example
  // {sql: 'to_point(?,?)', values: [1, 2]}
  const parts = ps.sql.split(PLACEHOLDER);
  const clause = [];
  for (let j = 0, m = parts.length; j < m; j++) {
    // Replace ? with the keyed placeholder, such as :5
    clause.push(parts[j]);
    if (j !== parts.length - 1) {
      clause.push(ps.params[j]); // Здесь вместо плейсхолдера вставляем само значение
    }
  }
  ps.sql = clause.join('');
  return ps;
};
/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
OdataDB.prototype.getPlaceholderForValue = function(key) {
  return key;
  // throw new Error(g.f('{{getPlaceholderForValue()}} must be implemented by ' +
  //  'the connector'));
};
/**
 * Build a new SQL statement with pagination support by wrapping the given sql
 * @param {String} model The model name
 * @param {ParameterizedSQL} stmt The sql statement
 * @param {Object} filter The filter object from the query
 */
OdataDB.prototype.applyPagination = function(model, stmt, filter) {
  if (filter.limit) stmt.top(filter.limit);
  if (filter.skip) stmt.skip(filter.skip);
  return stmt;
  // throw new Error(g.f('{{applyPagination()}} must be implemented by the connector'));
};
/**
 * Parse response data into Object from odata server
 *
 *
 * @param {Object} res The response text
 */
OdataDB.prototype.parse = function(res) {
  if (res.statusCode !== 200 && res.statusCode !== 201) {
    const error = new Error();
    let message;
    try {
      message = JSON.parse(res.body)['odata.error']['message']['value'];
    } catch (e) {
      message = res.body;
    }
    error.type = '1C';
    error.message = message;
    throw error;
  } else {
    if (res.body === '') return '';
    const result = JSON.parse(res.body);
    return result.value || result;
  }
};
/**
 * Transform the row data into a model data object
 * @param {string} model Model name
 * @param {object} rowData An object representing the row data from DB
 * @returns {object} Model data object
 */
OdataDB.prototype.fromRow = OdataDB.prototype.fromDatabase =
function(model, rowData, where) {
  if (rowData == null) {
    return rowData;
  }
  const props = this.getModelDefinition(model).properties;
  const data = {};
  for (const p in props) {
    const columnName = this.column(model, p);
    // Load properties from the row
    const columnValue = this.fromColumnValue(props[p], rowData[columnName]);
    if(props[p][NAME] && props[p][NAME].url) {
      data[p] = where[p];
    } else if (columnValue !== undefined) {
      data[p] = columnValue;
    }
  }
  return data;
};
/**
 * Convert the data from database column to model property
 * @param {object} propertyDef Model property definition
 * @param {*) value Column value
 * @returns {*} Model property value
 */
OdataDB.prototype.fromColumnValue = function(property, value) {
  if (value == null || !property) {
    return value;
  }

  if (!property.type.name && property.type.constructor.name === 'Array') {
    return value.map(row => this.fromRow(property.type[0].name, row));
  }

  if (property[NAME] && property[NAME].dataType && property[NAME].dataType === 'object') {
    return this.fromRow(property.type.name, value);
  }
  // Если это табличная часть dataType = table или property type array, то надо запустить рекурсивно по объекту
  switch (property.type.name) {
    case 'Number':
      return (+value);
    case 'Boolean':
      return value;
    case 'String':
      if (property.odata && property[NAME].dataType === 'enum' && property[NAME].values)
        return property[NAME].values[value] || value;
      else  return value;
    case 'Date':
      return (value === '0001-01-01T00:00:00') ? null: new Date(value);
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Array':
    case 'Object':
    case 'ModelConstructor':
    case 'JSON':
    default:
      return value;
  }
};

/**
 * Find matching model instances by the filter
 *
 * Please also note the name `all` is confusing. `Model.find` is to find all
 * matching instances while `Model.findById` is to find an instance by id. On
 * the other hand, `Connector.prototype.all` implements `Model.find` while
 * `Connector.prototype.find` implements `Model.findById` due to the `bad`
 * naming convention we inherited from juggling-db.
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [cb] The cb function
 */
OdataDB.prototype.all = function find(model, filter, options, cb) {
  const self = this;
  const headers = this.headers;
  filter = filter || {};
  const query = this.buildSelect(model, filter, options);
  query.get({headers})
  .then(res => {
    const data = this.parse(res);
    const objs = data.map(function(obj) {
      return self.fromRow(model, obj, filter.where);
    });
    if (filter && filter.include) {
      self.getModelDefinition(model).model.include(
        objs, filter.include, options, cb,
      );
    } else {
      cb(null, objs);
    }
  })
  .catch(err =>{
    return cb(err, []);
  });
};

/**
 * Build SQL expression
 * @param {String} columnName Escaped column name
 * @param {String} operator SQL operator
 * @param {*} columnValue Column value
 * @param {*} propertyValue Property value
 * @returns {ParameterizedSQL} The SQL expression
 */
OdataDB.prototype.buildExpression =
function(columnName, operator, columnValue, propertyValue) {
  function buildClause(columnValue, separator, grouping) {
    const values = [];
    for (let i = 0, n = columnValue.length; i < n; i++) {
      if (columnValue[i] instanceof ParameterizedSQL) {
        values.push(columnValue[i]);
      } else {
        values.push(new ParameterizedSQL(PLACEHOLDER, [columnValue[i]]));
      }
    }
    separator = separator || ',';
    const clause = ParameterizedSQL.join(values, separator);
    if (grouping) {
      clause.sql = '(' + clause.sql + ')';
    }
    return clause;
  }

  let sqlExp = columnName;
  let clause;
  if (columnValue instanceof ParameterizedSQL) {
    clause = columnValue;
  } else {
    clause = new ParameterizedSQL(PLACEHOLDER, [columnValue]);
  }
  switch (operator) {
    case 'gt':
      sqlExp += ' gt ';
      break;
    case 'gte':
      sqlExp += ' ge ';
      break;
    case 'lt':
      sqlExp += ' lt ';
      break;
    case 'lte':
      sqlExp += ' le ';
      break;
    case 'between':
      return new ParameterizedSQL(`${sqlExp} ge ${PLACEHOLDER} and ${sqlExp} le ${PLACEHOLDER}`, columnValue);
    case 'inq':
      sqlExp = '';
      clause = buildClause(columnValue.map(v => `${columnName} eq ${v}`), ' or ', true);
      break;
    case 'nin':
      sqlExp = '';
      clause = buildClause(columnValue.map(v => `${columnName} ne ${v}`), ' and ', true);
      break;
    case 'neq':
      if (columnValue == null) {
        return new ParameterizedSQL(sqlExp + ' IS NOT NULL');
      }
      sqlExp += ' ne ';
      break;
    case 'like':
      return new ParameterizedSQL(`like(${sqlExp}, ${PLACEHOLDER})`, [columnValue]);
    case 'nlike':
      sqlExp += ' NOT LIKE ';
      break;
    // this case not needed since each database has its own regex syntax, but
    // we leave the MySQL syntax here as a placeholder
    case 'regexp':
      sqlExp += ' REGEXP ';
      break;
  }
  const stmt = ParameterizedSQL.join([sqlExp, clause], '');
  return stmt;
};

Connector.defineAliases(OdataDB.prototype, 'all', ['findAll']);

/**
 * Create the data model in MySQL
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
OdataDB.prototype.create = function(model, data, options, callback) {
  const self = this;
  const headers = this.headers;
  const query = this.buildInsert(model, data, options);
  query.post(query.data, {headers})
  .then(res => {
    /* const result = this.parse(res);
    callback(null, result); */
    const data = this.parse(res);
    const objs = self.fromRow(model, data);
    const idName = this.idName(model);
    callback(null, idName && objs[idName]);
    
  })
  .catch(err =>{
    return callback(err);
  });
};

/**
 * Build the the `INSERT INTO` statement
 * @param {String} model The model name
 * @param {Object} fields Fields to be inserted
 * @param {Object} options Options object
 * @returns {ParameterizedSQL}
 */
/* OdataDB.prototype.buildInsertInto = function(model, fields, options) {
  const stmt = new ParameterizedSQL('INSERT INTO ' + this.tableEscaped(model));
  const columnNames = fields.names.join(',');
  if (columnNames) {
    stmt.merge('(' + columnNames + ')', '');
  }
  return stmt;
}; */

/**
 * Parse the result for SQL INSERT for newly inserted id
 * @param {String} model Model name
 * @param {Object} info The status object from driver
 * @returns {*} The inserted id value
 */
OdataDB.prototype.getInsertedId = function(model, info) {
  throw new Error(g.f('{{getInsertedId()}} must be implemented by the connector'));
};

/**
 * Build INSERT SQL statement
 * @param {String} model The model name
 * @param {Object} data The model data object
 * @param {Object} options The options object
 * @returns {string} The INSERT SQL statement
 */
OdataDB.prototype.buildInsert = function(model, data, options) {
  const resource = encodeURI(this.tableEscaped(model));

  let query = this.odata({service: this.settings.url,
    format: 'json',
  });

  query.resource(resource);

  const fields = this.buildFields(model, data);

  query.data = fields.data;

  // TODO default values
  // TODO nested object keys

/*   const columnValues = fields.columnValues;
  const fieldNames = fields.names; */
  /* if (fieldNames.length) {
    const values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    insertStmt.merge(values);
  } else {
    insertStmt.merge(this.buildInsertDefaultValues(model, data, options));
  } */
/*   const returning = this.buildInsertReturning(model, data, options);
  if (returning) {
    insertStmt.merge(returning);
  } */
  return query;
};

/**
 * Build an array of fields for the database operation
 * @param {String} model Model name
 * @param {Object} data Model data object
 * @param {Boolean} excludeIds Exclude id properties or not, default to false
 * @returns {{names: Array, values: Array, properties: Array}}
 */
OdataDB.prototype.buildFields = function(model, data, excludeIds) {
  const keys = Object.keys(data);
  return this._buildFieldsForKeys(model, data, keys, excludeIds);
};

/*
 * @param {String} model The model name.
 * @returns {Object} data The model data object.
 * @returns {Array} keys The key fields for which need to be built.
 * @param {Boolean} excludeIds Exclude id properties or not, default to false
 * @private
 */

OdataDB.prototype.propToColumnValue = function(model, p, value) {
  const props = this.getModelDefinition(model).properties;
  return this.toColumnValue(props[p], value, false);
}

/*
 * @param {String} model The model name.
 * @returns {Object} data The model data object.
 * @returns {Array} keys The key fields for which need to be built.
 * @param {Boolean} excludeIds Exclude id properties or not, default to false
 * @private
 */
OdataDB.prototype._buildFieldsForKeys = function(model, data, keys, excludeIds) {
  const props = this.getModelDefinition(model).properties;
  const fields = {
    names: [], // field names
    columnValues: [], // an array
    properties: [], // model properties
    data: {}, // aggregated data object
  };
  for (let i = 0, n = keys.length; i < n; i++) {
    const key = keys[i];
    const p = props[key];
    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }

    if (excludeIds && p.id) {
      continue;
    }
    const k = this.columnEscaped(model, key);
    const v = this.toColumnValue(p, data[key], true);
    //const v = data[key];
    if (v !== undefined) {
      fields.names.push(k);
      fields.columnValues.push(v);
      fields.properties.push(p);
      fields.data[k] = v;
    }
  }
  return fields;
};

/**
 * Build the clause to return id values after insert
 * @param {String} model The model name
 * @param {Object} data The model data object
 * @param {Object} options Options object
 * @returns {string}
 */
OdataDB.prototype.buildInsertReturning = function(model, data, options) {
  return '';
};

/*!
 * Check if id value is set
 * @param idValue
 * @param cb
 * @param returningNull
 * @returns {boolean}
 */
function isIdValuePresent(idValue, cb, returningNull) {
  try {
    assert(idValue !== null && idValue !== undefined, 'id value is required');
    return true;
  } catch (err) {
    process.nextTick(function() {
      if (cb) cb(returningNull ? null : err);
    });
    return false;
  }
}

/**
 * ATM, this method is not used by loopback-datasource-juggler dao, which
 * maps `updateAttributes` to `update` with a `where` filter that includes the
 * `id` instead.
 *
 * Update attributes for a given model instance
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Object} data The model data instance containing all properties to
 * be updated
 * @param {Object} options Options object
 * @param {Function} cb The callback function
 * @private
 */
OdataDB.prototype.updateAttributes = function(model, id, data, options, cb) {
  if (!isIdValuePresent(id, cb)) return;
  const self = this;
  const headers = this.headers;
  const where = this._buildWhereObjById(model, id, data);
  const query = this.buildUpdateById(model, where, data, options);
  query.patch(query.data, {headers})
  .then(res => {
    const result = this.parse(res);
    cb(null, result);
  })
  .catch(err =>{
    return cb(err);
  });
};

/*
 * @param model The model name.
 * @param id The instance ID.
 * @param {Object} data The data Object.
 * @returns {Object} where The where object for a spcific instance.
 * @private
 */
OdataDB.prototype._buildWhereObjById = function(model, id, data) {
  const idName = this.idName(model);
  delete data[idName];
  const where = {};
  where[idName] = id;
  return where;
};


/**
 * Build INSERT SQL statement
 * @param {String} model The model name
 * @param {Object} where The model instance id object
 * @param {Object} data The model data object
 * @param {Object} options The options object
 * @returns {string} The INSERT SQL statement
 */
OdataDB.prototype.buildUpdateById = function(model, where, data, options) {
  const resource = encodeURI(this.tableEscaped(model));

  let query = this.odata({service: this.settings.url,
    format: 'json',
  });

  const id = this.buildFields(model, where);
  if(id.names.length === 1) {
    query.resource(`${resource}(${id.columnValues[0]})`);
  } else {
    throw new Error('Multiply id`s not supported yet')
  }
  
  const fields = this.buildFields(model, data);
  query.data = fields.data;

  return query;
};


/**
 * Create the data model in MySQL
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
/* OdataDB.prototype.update = function(model, where, data, options, callback) {
  const self = this;
  const headers = this.headers;
  const query = this.buildUpdate(model, data, options);
  query.patch(query.data, {headers})
  .then(res => {
    const result = this.parse(res);
    callback(null, result);
  })
  .catch(err =>{
    return callback(err);
  });
}; */

//Connector.defineAliases(OdataDB.prototype, 'update', ['updateAll']);


/**
 * Count all model instances by the where filter
 *
 * @param {String} model The model name
 * @param {Object} where The where object
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
OdataDB.prototype.count = function(model, where, options, cb) {
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    const tmp = options;
    cb = where;
    where = tmp;
  }
  const self = this;
  const headers = this.headers;  
  const query = this.buildSelect(model, {where}, options, true);
  query.get({headers})
  .then(res => {
    const data = this.parse(res);    
    cb(null, data);
  })
  .catch(err =>{
    return cb(err, -1);
  });
};