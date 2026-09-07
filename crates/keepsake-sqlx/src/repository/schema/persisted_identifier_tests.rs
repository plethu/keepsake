use super::{PERSISTED_IDENTIFIERS, validate_persisted_identifier_bytes};

#[test]
fn byte_reader_matches_core_identifier_contract() {
    let identifier = PERSISTED_IDENTIFIERS[0];
    let valid = format!("{}a", "é".repeat(95));
    assert_eq!(valid.len(), 191);
    assert!(validate_persisted_identifier_bytes(identifier, "row-1", valid.as_bytes()).is_ok());

    for (value, expected) in [
        ("", "must not be empty"),
        ("\u{2003}tenant", "leading or trailing whitespace"),
        ("tenant\u{2003}", "leading or trailing whitespace"),
        ("tenant\u{007f}", "control character"),
        ("tenant\u{fdd0}", "noncharacter"),
    ] {
        let result = validate_persisted_identifier_bytes(identifier, "row-2", value.as_bytes());
        assert!(result.is_err(), "{value:?} should be rejected");
        if let Err(error) = result {
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    let too_long = "é".repeat(96);
    let result = validate_persisted_identifier_bytes(identifier, "row-3", too_long.as_bytes());
    assert!(result.is_err(), "byte length should be bounded");
    if let Err(error) = result {
        assert!(error.to_string().contains("192 UTF-8 bytes"), "{error}");
    }

    let result = validate_persisted_identifier_bytes(identifier, "row-4", &[0xff]);
    assert!(result.is_err(), "invalid UTF-8 should be rejected");
    if let Err(error) = result {
        assert!(error.to_string().contains("invalid UTF-8"), "{error}");
    }
}
