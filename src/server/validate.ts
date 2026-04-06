export interface ValidationError
{
    error: string
}

export function requireString(body: unknown, field: string): string | ValidationError
{
    if (typeof body !== "object" || body === null || Array.isArray(body))
        return {error: "body must be an object"}

    const value = (body as Record<string, unknown>)[field]
    if (typeof value !== "string" || value.length === 0)
        return {error: `${field} must be a non-empty string`}

    return value
}

export function isValidationError(x: string | ValidationError): x is ValidationError
{
    return typeof x !== "string"
}
