import { helper, getSetArgs, escapeName } from "../utils.js";

import type {
	Adapter,
	InitializeAdapter,
	UserSchema,
	SessionSchema,
	KeySchema
} from "lucia";
import type {
	QueryResult,
	DatabaseError,
	Pool,
	PoolClient,
	QueryResultRow
} from "pg";

export type TableConfig = {
		user: string;
		session: string;
		key: string;
};

class ProviderOps<T extends Pool | PoolClient> {
	pool: T;

	constructor(pool: T) {
		this.pool = pool;
	}

	async exec(query: string, arg?: any[]) {
		return this.pool.query(query, arg);
	}

	async get<U extends QueryResultRow>(query: string, arg?: any[]) {
		return get<U>(this.pool.query(query, arg));
	}

	async getAll<U extends QueryResultRow>(query: string, arg?: any[]) {
		return getAll<U>(this.pool.query(query, arg));
	}
}

type TXHandler = (fn: ProviderOps<PoolClient>) => Promise<any>;

class provider extends ProviderOps<Pool> {
	constructor(pool: Pool) {
		super(pool);
	}

	async transaction(execute: TXHandler) {
		const p = new ProviderOps(await this.pool.connect());
		try {
			await p.exec("BEGIN");
			await execute(p);
			await p.exec("COMMIT");
		} catch (e) {
			p.exec("ROLLBACK");
			throw e;
		}
	}
}

export const pgAdapter = (
	pool: Pool,
	tables: TableConfig
): InitializeAdapter<Adapter> => {
	const db = new provider(pool);

	const ESCAPED_USER_TABLE_NAME = escapeName(tables.user);
	const ESCAPED_SESSION_TABLE_NAME = escapeName(tables.session);
	const ESCAPED_KEY_TABLE_NAME = escapeName(tables.key);

	return (LuciaError) => {
		return {
			getUser: async (userId) => {
				const result = await db.get<UserSchema>(
					`SELECT * FROM ${ESCAPED_USER_TABLE_NAME} WHERE id = $1`,
					[userId]
				);

				return result;
			},
			setUser: async (user, key) => {
				if (!key) {
					const [userFields, userValues, userArgs] = helper(user);
					await db.exec(
						`INSERT INTO ${ESCAPED_USER_TABLE_NAME} ( ${userFields} ) VALUES ( ${userValues} )`,
						userArgs
					);
					return;
				}
				try {
					await db.transaction(async (tx) => {
						const [userFields, userValues, userArgs] = helper(user);
						await tx.exec(
							`INSERT INTO ${ESCAPED_USER_TABLE_NAME} ( ${userFields} ) VALUES ( ${userValues} )`,
							userArgs
						);
						const [keyFields, keyValues, keyArgs] = helper(key);
						await tx.exec(
							`INSERT INTO ${ESCAPED_KEY_TABLE_NAME} ( ${keyFields} ) VALUES ( ${keyValues} )`,
							keyArgs
						);
					});
				} catch (e) {
					const error = e as Partial<DatabaseError>;
					if (error.code === "23505" && error.detail?.includes("Key (id)")) {
						throw new LuciaError("AUTH_DUPLICATE_KEY_ID");
					}
					throw e;
				}
			},
			deleteUser: async (userId) => {
				await db.exec(`DELETE FROM ${ESCAPED_USER_TABLE_NAME} WHERE id = $1`, [
					userId
				]);
			},
			updateUser: async (userId, partialUser) => {
				const [fields, values, args] = helper(partialUser);
				await db.exec(
					`UPDATE ${ESCAPED_USER_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, userId]
				);
			},

			getSession: async (sessionId) => {
				const result = await db.get<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);
				return result ? transformPgSession(result) : null;
			},
			getSessionsByUserId: async (userId) => {
				const result = await db.getAll<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
				return result.map((val) => transformPgSession(val));
			},
			setSession: async (session) => {
				try {
					const [fields, values, args] = helper(session);
					await db.exec(
						`INSERT INTO ${ESCAPED_SESSION_TABLE_NAME} ( ${fields} ) VALUES ( ${values} )`,
						args
					);
				} catch (e) {
					const error = e as Partial<DatabaseError>;
					if (
						error.code === "23503" &&
						error.detail?.includes("Key (user_id)")
					) {
						throw new LuciaError("AUTH_INVALID_USER_ID");
					}
					throw e;
				}
			},
			deleteSession: async (sessionId) => {
				await db.exec(
					`DELETE FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);
			},
			deleteSessionsByUserId: async (userId) => {
				await db.exec(
					`DELETE FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
			},
			updateSession: async (sessionId, partialSession) => {
				const [fields, values, args] = helper(partialSession);
				await db.exec(
					`UPDATE ${ESCAPED_SESSION_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, sessionId]
				);
			},

			getKey: async (keyId) => {
				const result = await db.get<KeySchema>(
					`SELECT * FROM ${ESCAPED_KEY_TABLE_NAME} WHERE id = $1`,
					[keyId]
				);
				return result;
			},
			getKeysByUserId: async (userId) => {
				const result = db.getAll<KeySchema>(
					`SELECT * FROM ${ESCAPED_KEY_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
				return result;
			},
			setKey: async (key) => {
				try {
					const [fields, values, args] = helper(key);
					await db.exec(
						`INSERT INTO ${ESCAPED_KEY_TABLE_NAME} ( ${fields} ) VALUES ( ${values} )`,
						args
					);
				} catch (e) {
					const error = e as Partial<DatabaseError>;
					if (
						error.code === "23503" &&
						error.detail?.includes("Key (user_id)")
					) {
						throw new LuciaError("AUTH_INVALID_USER_ID");
					}
					if (error.code === "23505" && error.detail?.includes("Key (id)")) {
						throw new LuciaError("AUTH_DUPLICATE_KEY_ID");
					}
					throw e;
				}
			},
			deleteKey: async (keyId) => {
				await db.exec(`DELETE FROM ${ESCAPED_KEY_TABLE_NAME} WHERE id = $1`, [
					keyId
				]);
			},
			deleteKeysByUserId: async (userId) => {
				await db.exec(
					`DELETE FROM ${ESCAPED_KEY_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
			},
			updateKey: async (keyId, partialKey) => {
				const [fields, values, args] = helper(partialKey);
				await db.exec(
					`UPDATE ${ESCAPED_KEY_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, keyId]
				);
			},

			getSessionAndUser: async (sessionId) => {
				const getSessionPromise = db.get<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);

				const getUserFromJoinPromise = db.get<
					UserSchema & {
						__session_id: string;
					}
				>(
					`SELECT ${ESCAPED_USER_TABLE_NAME}.*, ${ESCAPED_SESSION_TABLE_NAME}.id as __session_id FROM ${ESCAPED_SESSION_TABLE_NAME} INNER JOIN ${ESCAPED_USER_TABLE_NAME} ON ${ESCAPED_USER_TABLE_NAME}.id = ${ESCAPED_SESSION_TABLE_NAME}.user_id WHERE ${ESCAPED_SESSION_TABLE_NAME}.id = $1`,
					[sessionId]
				);

				const [sessionResult, userFromJoinResult] = await Promise.all([
					getSessionPromise,
					getUserFromJoinPromise
				]);
				if (!sessionResult || !userFromJoinResult) return [null, null];
				const { __session_id: _, ...userResult } = userFromJoinResult;
				return [transformPgSession(sessionResult), userResult];
			}
		};
	};
};

export const get = async <_Schema extends QueryResultRow>(
	queryPromise: Promise<QueryResult<_Schema>>
): Promise<_Schema | null> => {
	const { rows } = await queryPromise;
	const result = rows.at(0) ?? null;
	return result;
};

export const getAll = async <_Schema extends QueryResultRow>(
	queryPromise: Promise<QueryResult<_Schema>>
): Promise<_Schema[]> => {
	const { rows } = await queryPromise;
	return rows;
};

export type PgSession = Omit<
	SessionSchema,
	"active_expires" | "idle_expires"
> & {
	active_expires: BigInt;
	idle_expires: BigInt;
};

export const transformPgSession = (session: PgSession): SessionSchema => {
	return {
		...session,
		active_expires: Number(session.active_expires),
		idle_expires: Number(session.idle_expires)
	};
};
