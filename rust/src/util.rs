

// might should be copyable too
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KeyValueMetadata {
    keys: Vec<String>,
    values: Vec<String>
}


impl KeyValueMetadata {

    pub fn new<I, S>(keys: I, values: I) -> KeyValueMetadata
        where I: IntoIterator<Item = S>,
              S: Into<String>
    {
        KeyValueMetadata {
            keys: keys.into_iter().map(|s| s.into()).collect(),
            values: values.into_iter().map(|s| s.into()).collect(),
        }
    }

}


#[cfg(test)]
mod tests {
    use util::KeyValueMetadata;

    #[test]
    fn test_from_string_vector() {
        let keys = vec!["foo", "bar"];
        let values = vec!["bizz", "buzz"];

        let meta = KeyValueMetadata::new(keys, values);
    }

}
