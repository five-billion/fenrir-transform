import _ = require("lodash")

/**
 * Deep diff between two object-likes
 * @param  {Object} fromObject the original object
 * @param  {Object} toObject   the updated object
 * @return {Object}            a new object which represents the diff
 */
export function deepDiff(fromObject: Record<string, any> | object, toObject: Record<string, any>) {
  const changes: Record<string, any> = {}

  const buildPath = (path: string | undefined, _obj: unknown, key: string) =>
    _.isUndefined(path) ? key : `${path}.${key}`

  const walk = (fromObject: object | Record<string, any>, toObject: Record<string, any>, path?: string) => {
    for (const key of _.keys(fromObject)) {
      const currentPath: string = buildPath(path, fromObject, key)
      if (!_.has(toObject, key)) {
        changes[currentPath] = { from: _.get(fromObject, key) }
      }
    }

    for (const [key, to] of _.entries(toObject)) {
      const currentPath = buildPath(path, toObject, key)
      if (!_.has(fromObject, key)) {
        changes[currentPath] = { to }
      } else {
        const from = _.get(fromObject, key)
        if (!_.isEqual(from, to)) {
          if (_.isObjectLike(to) && _.isObjectLike(from)) {
            walk(from, to, currentPath)
          } else {
            changes[currentPath] = { from, to }
          }
        }
      }
    }
  }

  walk(fromObject, toObject)

  return changes
}