"""Pagination control model for DirectoryService."""

PAGINATION_MODEL = {
    "describe_directories": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 100,  # This should be the sum of the directory limits
        "unique_attribute": "directory_id",
    },
    "list_tags_for_resource": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 50,
        "unique_attribute": "Key",
    },
    "describe_ldaps_settings": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 100,
    },
    "describe_trusts": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 100,
        "unique_attribute": "trust_id",
    },
    "describe_settings": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 100,
        "unique_attribute": "Name",
    },
}

# List of directory security settings
# https://docs.aws.amazon.com/directoryservice/latest/admin-guide/ms_ad_directory_settings.html#list-ds-settings
SETTINGS_ENTRIES_MODEL = [
    {
        "Type": "Protocol",
        "Name": "PCT_1_0",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Protocol",
        "Name": "SSL_2_0",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Protocol",
        "Name": "SSL_3_0",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Protocol",
        "Name": "TLS_1_0",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Protocol",
        "Name": "TLS_1_1",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "AES_128_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "DES_56_56",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC2_40_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC2_56_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC2_128_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC4_40_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC4_56_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC4_64_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "RC4_128_128",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Cipher",
        "Name": "3DES_168_168",
        "AllowedValues": '["Enable","Disable"]',
        "AppliedValue": "Enable",
        "RequestStatus": "Default",
        "DataType": "Boolean",
    },
    {
        "Type": "Certificate-Based Authentication",
        "Name": "CERTIFICATE_BACKDATING_COMPENSATION",
        "AllowedValues": '{"MinimumValue": 1, "MaximumValue": 1577000000}',
        "AppliedValue": "600",
        "RequestStatus": "Default",
        "DataType": "DurationInSeconds",
    },
    {
        "Type": "Certificate-Based Authentication",
        "Name": "CERTIFICATE_STRONG_ENFORCEMENT",
        "AllowedValues": '["Compatibility","Full_Enforcement"]',
        "AppliedValue": "Compatibility",
        "RequestStatus": "Default",
        "DataType": "Enum",
    },
]
