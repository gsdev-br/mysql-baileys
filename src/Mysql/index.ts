import { createConnection } from 'mysql2/promise'
import { BufferJSON, initAuthCreds, fromObject } from '../Utils'
import { MySQLConfig, sqlData, sqlConnection, AuthenticationCreds, AuthenticationState, SignalDataTypeMap } from '../Types'

let conn: sqlConnection

async function connection(config: MySQLConfig, force: boolean = false) {
  const ended = !!conn?.connection?._closing
  const newConnection = conn === undefined

  if (newConnection || ended || force) {
    conn = await createConnection({
      database: config.database || 'base',
      host: config.host || 'localhost',
      port: config.port || 3306,
      user: config.user || 'root',
      password: config.password,
      password1: config.password1,
      password2: config.password2,
      password3: config.password3,
      enableKeepAlive: true,
      keepAliveInitialDelay: 5000,
      ssl: config.ssl,
      localAddress: config.localAddress,
      socketPath: config.socketPath,
      insecureAuth: config.insecureAuth || false,
      isServer: config.isServer || false
    })

    if (newConnection) {
      await conn.execute(`
        CREATE TABLE IF NOT EXISTS \`${config.tableName || 'auth'}\` (
          \`id\` VARCHAR(80) NOT NULL PRIMARY KEY,
          \`value\` JSON DEFAULT NULL
        ) ENGINE=MyISAM;
      `)
    }
  }

  return conn
}

export const useMySQLAuthState = async (config: MySQLConfig): Promise<{
  state: AuthenticationState,
  saveCreds: () => Promise<void>,
  clear: () => Promise<void>,
  removeCreds: () => Promise<void>,
  dropTable: () => Promise<void>,
  query: (sql: string, values: string[]) => Promise<sqlData>
}> => {
  const sqlConn = await connection(config)
  const tableName = config.tableName || 'auth'
  const retryRequestDelayMs = config.retryRequestDelayMs || 200
  const maxtRetries = config.maxtRetries || 10

  const query = async (sql: string, values: string[]) => {
    for (let x = 0; x < maxtRetries; x++) {
      try {
        const [rows] = await sqlConn.query(sql, values)
        return rows as sqlData
      } catch (e) {
        await new Promise(r => setTimeout(r, retryRequestDelayMs))
      }
    }
    return [] as sqlData
  }

  const readData = async (id: string) => {
    const data = await query(`SELECT value FROM ${tableName} WHERE id = ?`, [id])
    if (!data[0]?.value) return null
    const raw = typeof data[0].value === 'object' ? JSON.stringify(data[0].value) : data[0].value
    return JSON.parse(raw, BufferJSON.reviver)
  }

  const writeData = async (id: string, value: object) => {
    const valueFixed = JSON.stringify(value, BufferJSON.replacer)
    await query(
      `INSERT INTO ${tableName} (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?`,
      [id, valueFixed, valueFixed]
    )
  }

  const removeData = async (id: string) => {
    await query(`DELETE FROM ${tableName} WHERE id = ?`, [id])
  }

  const clearAll = async () => {
    await query(`DELETE FROM ${tableName} WHERE id != 'creds'`, [])
  }

  const removeAll = async () => {
    await query(`DELETE FROM ${tableName}`, [])
  }

  const creds: AuthenticationCreds = await readData('creds') || initAuthCreds()

  return {
    state: {
      creds: creds,
      keys: {
        get: async (type, ids) => {
          const data: { [id: string]: SignalDataTypeMap[typeof type] } = {}
          for (const id of ids) {
            let value = await readData(`${type}-${id}`)
            if (type === 'app-state-sync-key' && value) {
              value = fromObject(value)
            }
            data[id] = value
          }
          return data
        },
        set: async (data) => {
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id]
              const key = `${category}-${id}`
              if (value) {
                await writeData(key, value)
              } else {
                await removeData(key)
              }
            }
          }
        }
      }
    },
    saveCreds: async () => {
      await writeData('creds', creds)
    },
    clear: async () => {
      await clearAll()
    },
    removeCreds: async () => {
      await removeAll()
    },
    dropTable: async () => {
      await query(`DROP TABLE IF EXISTS ${tableName}`, [])
    },
    query
  }
}
