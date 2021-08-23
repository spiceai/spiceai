export const getApiPath = (path: string): string => {
    return `/api/${process.env.REACT_APP_SPICE_API_VERSION}${path}`
}