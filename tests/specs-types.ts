import type { Task } from "../src/types"

export type PresenceFieldEntry = string | [string, PresenceFieldList]
export type PresenceFieldList = PresenceFieldEntry[]

export type PresenceValue = boolean | PresenceObject
export interface PresenceObject {
  [key: string]: PresenceValue;
}

export type PresenceOrValue<T> = {
  [K in keyof T]: T[K] extends (Record<string, any> | undefined)
    ? PresenceOrValue<T[K]> | boolean
    : T[K] | boolean
};
