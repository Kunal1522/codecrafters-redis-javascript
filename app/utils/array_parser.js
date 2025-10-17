function toRESP(value, type = "bulk") {
    if (value === null || value === undefined) {
        return "$-1\r\n"; // Null bulk string
    }
    if (typeof value === "string") {
        if (type === "simple") {
            return `+${value}\r\n`; // Simple string
        }
        return `$${value.length}\r\n${value}\r\n`; // Bulk string
    }
    if (typeof value === "number") {
        return `:${value}\r\n`; // Integer
    }
    if (Array.isArray(value)) {
        const arrayItems = value.map(toRESP).join(""); // Recursively format array items
        return `*${value.length}\r\n${arrayItems}`;
    }
    if (typeof value === "object") {
        return `$${JSON.stringify(value).length}\r\n${JSON.stringify(value)}\r\n`; // Serialize objects as JSON
    }
    throw new Error("Unsupported value type");
}

export {toRESP};
