import { SPICE_API_VERSION } from '../constants'

export const getApiPath = (path: string): string => {
    return `/api/${SPICE_API_VERSION}${path}`
}