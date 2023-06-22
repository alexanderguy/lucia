import { helper, getSetArgs, escapeName } from "../utils.js";

import type {
	Adapter,
	InitializeAdapter,
	UserSchema,
	SessionSchema,
	KeySchema
} from "lucia";

export type TableConfig = {
	user: string;
	session: string;
	key: string;
};

export interface IProviderError {
	code?: string;
	detail?: string;
}

export interface IProviderOps {
	exec(query: string, arg?: any[]): Promise<unknown>;
	get<T>(query: string, arg?: any[]): Promise<T>;
	getAll<T>(query: string, arg?: any[]): Promise<T[]>;
	processException<T>(e: T): IProviderError;
}

export type ITXHandler = (fn: IProviderOps) => Promise<unknown>;

export interface IProvider extends IProviderOps {
	transaction(fn: ITXHandler): Promise<any>;
}

export const providerAdapter = (
	p: IProvider,
	tables: any
): InitializeAdapter<Adapter> => {
	const ESCAPED_USER_TABLE_NAME = escapeName(tables.user);
	const ESCAPED_SESSION_TABLE_NAME = escapeName(tables.session);
	const ESCAPED_KEY_TABLE_NAME = escapeName(tables.key);

	return (LuciaError) => {
		return {
			getUser: async (userId) => {
				const result = await p.get<UserSchema>(
					`SELECT * FROM ${ESCAPED_USER_TABLE_NAME} WHERE id = $1`,
					[userId]
				);

				return result;
			},
			setUser: async (user, key) => {
				if (!key) {
					const [userFields, userValues, userArgs] = helper(user);
					await p.exec(
						`INSERT INTO ${ESCAPED_USER_TABLE_NAME} ( ${userFields} ) VALUES ( ${userValues} )`,
						userArgs
					);
					return;
				}
				try {
					await p.transaction(async (tx) => {
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
					const error = p.processException(e);
					if (error.code === "23505" && error.detail?.includes("Key (id)")) {
						throw new LuciaError("AUTH_DUPLICATE_KEY_ID");
					}
					throw e;
				}
			},
			deleteUser: async (userId) => {
				await p.exec(`DELETE FROM ${ESCAPED_USER_TABLE_NAME} WHERE id = $1`, [
					userId
				]);
			},
			updateUser: async (userId, partialUser) => {
				const [fields, values, args] = helper(partialUser);
				await p.exec(
					`UPDATE ${ESCAPED_USER_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, userId]
				);
			},

			getSession: async (sessionId) => {
				const result = await p.get<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);
				return result ? transformPgSession(result) : null;
			},
			getSessionsByUserId: async (userId) => {
				const result = await p.getAll<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
				return result.map((val) => transformPgSession(val));
			},
			setSession: async (session) => {
				try {
					const [fields, values, args] = helper(session);
					await p.exec(
						`INSERT INTO ${ESCAPED_SESSION_TABLE_NAME} ( ${fields} ) VALUES ( ${values} )`,
						args
					);
				} catch (e) {
					const error = p.processException(e);
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
				await p.exec(
					`DELETE FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);
			},
			deleteSessionsByUserId: async (userId) => {
				await p.exec(
					`DELETE FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
			},
			updateSession: async (sessionId, partialSession) => {
				const [fields, values, args] = helper(partialSession);
				await p.exec(
					`UPDATE ${ESCAPED_SESSION_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, sessionId]
				);
			},

			getKey: async (keyId) => {
				const result = await p.get<KeySchema>(
					`SELECT * FROM ${ESCAPED_KEY_TABLE_NAME} WHERE id = $1`,
					[keyId]
				);
				return result;
			},
			getKeysByUserId: async (userId) => {
				const result = p.getAll<KeySchema>(
					`SELECT * FROM ${ESCAPED_KEY_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
				return result;
			},
			setKey: async (key) => {
				try {
					const [fields, values, args] = helper(key);
					await p.exec(
						`INSERT INTO ${ESCAPED_KEY_TABLE_NAME} ( ${fields} ) VALUES ( ${values} )`,
						args
					);
				} catch (e) {
					const error = p.processException(e);
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
				await p.exec(`DELETE FROM ${ESCAPED_KEY_TABLE_NAME} WHERE id = $1`, [
					keyId
				]);
			},
			deleteKeysByUserId: async (userId) => {
				await p.exec(
					`DELETE FROM ${ESCAPED_KEY_TABLE_NAME} WHERE user_id = $1`,
					[userId]
				);
			},
			updateKey: async (keyId, partialKey) => {
				const [fields, values, args] = helper(partialKey);
				await p.exec(
					`UPDATE ${ESCAPED_KEY_TABLE_NAME} SET ${getSetArgs(
						fields,
						values
					)} WHERE id = $${fields.length + 1}`,
					[...args, keyId]
				);
			},

			getSessionAndUser: async (sessionId) => {
				const getSessionPromise = p.get<PgSession>(
					`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME} WHERE id = $1`,
					[sessionId]
				);

				const getUserFromJoinPromise = p.get<
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
