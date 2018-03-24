use std::fmt;
use util::KeyValueMetadata;


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TimeUnit {
    Second,
    Milli,
    Micro,
    Nano
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateUnit {
    Day,
    Milli
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum IntervalUnit {
    YearMonth,
    DayTime
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Precision {
    Half,
    Single,
    Double
}

// TODO consider using the following approach: https://github.com/rust-lang-nursery/stdsimd/blob/548abdc6af5bdfcf12a22d44eebcd77ff2d49f89/crates/stdsimd-verify/tests/x86-intel.rs#L31

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DataType {
    NA,
    Bool,

    Int8,
    Int16,
    Int32,
    Int64,

    UInt8,
    UInt16,
    UInt32,
    UInt64,

    HalfFloat,
    Float,
    Double,

    String,
    Binary,
    FixedSizedBinary(i32),  // byte_width
    // might prefer tuple variants instead of struct ones
    Time32(TimeUnit),
    Time64(TimeUnit),
    Date32(TimeUnit),
    Date64(TimeUnit),
    Timestamp {
        unit: TimeUnit,
        // makes uncopyable this variant, might try to use a non-string timezeone, like variants from chro
        timezone: String
    },
    Interval(IntervalUnit),
    Decimal {
        precision: i32,
        scale: i32
    },
    List(Box<DataType>),
    Struct,
    Union,
    Dictionary,
    Map
}

// Kind enum
// TODO datatype kind enum


#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Field {
    name: String,
    dtype: DataType, // set lifetime
    nullable: bool,
    metadata: Option<KeyValueMetadata>
}


//TODO: use indexmap instead
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Schema {
    fields: Vec<Field>,
    //name_to_index: HashMap<String, usize>,
    metadata: Option<KeyValueMetadata>
}


impl DataType {
    pub fn bit_width(&self) -> i32 {
        use self::DataType::*;
        match *self {
            Bool => 1,

            Int8 | UInt8 => 8,
            Int16 | UInt16 | HalfFloat => 16,
            Int32 | UInt32 | Float => 32,
            Int64 | UInt64 | Double => 64,

            Time32(_) | Date32(_) => 32,
            Time64(_) | Date64(_) | Interval(_) => 64,

            FixedSizedBinary(bytes) => bytes * 8,

            //Timestamp { .. } => 64,
            //Interval { .. } => 64,

            //Decimal { .. } => 16 * 8,

            //Dictionary { ref index_type, .. } => index_type.bit_width(),

            _ => panic!("{:?} is not fixed width type", self)
        }
    }

    pub fn name(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }

    // try_from(str)
}

impl fmt::Display for DataType {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::DataType::*;
        let text = match *self {
            List(ref dtype) => format!("list<item: {}>", dtype.name()),
            _ => self.name()
        };
        write!(f, "{}", text)
    }

}


impl Field {

    pub fn new(name: &str, dtype: DataType, nullable: bool) -> Field {
        Field {
            name: String::from(name),
            dtype: dtype,
            nullable: nullable,
            metadata: None
        }
    }

    //TODO nullable factory

    pub fn with_metadata(name: &str, dtype: DataType, nullable: bool,
                         metadata: KeyValueMetadata) -> Field {
        Field {
            name: String::from(name),
            dtype: dtype,
            nullable: nullable,
            metadata: Some(metadata)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn metadata(&self) -> &Option<KeyValueMetadata> {
        &self.metadata
    }

    // TODO from datatype

}


impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let postfix = if self.nullable { "" } else { " not null" };
        write!(f, "{}: {}{}", self.name, self.dtype, postfix)
    }
}


// TODO: better ergonomics
// newtype instead of struct
// implement from_iter
impl Schema {

    pub fn new<I>(fields: I) -> Schema
        where I: IntoIterator<Item = Field>
    {
        Schema {
            fields: fields.into_iter().collect(),
            metadata: None
        }
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&Field> {
        // TODO: arrow builds a name_to_index mapping on first use
        self.fields.iter().find(|f| f.name() == name)
    }
}


impl fmt::Display for Schema {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let fields: Vec<String> = self.fields.iter().map(|f| f.to_string()).collect();
        write!(f, "({})", fields.join("\n "))
    }

}


#[cfg(test)]
mod tests {
    use std::mem;
    use super::*;
    use super::DataType::*;


    #[test]
    fn test_field_basics() {
        let f0 = Field::new("f0", Int32, true);
        let f0_nn = Field::new("f0", Int32, false);

        assert_eq!(f0.name(), "f0");
        assert_eq!(f0.dtype(), &Int32);
        assert_eq!(f0.nullable(), true);
        assert_eq!(f0_nn.nullable(), false)
    }

    #[test]
    fn test_field_equals() {
        let f0 = Field::new("f0", Int32, true);
        let f0_nn = Field::new("f0", Int32, false);
        let f0_other = Field::new("f0", Int32, true);

        assert!(f0 == f0_other);
        assert!(f0 != f0_nn);

    }

    #[test]
    fn test_field_with_metadata() {
        let metadata = KeyValueMetadata::new(vec!["foo", "bar"],
                                             vec!["bizz", "buzz"]);
        let f0 = Field::with_metadata("f0", Int32, true, metadata.clone());
        let f1 = Field::new("f0", Int32, true);
        assert_eq!(f0.metadata(), &Some(metadata));
        assert!(f0 != f1);
    }

    #[test]
    fn test_schema_basics() {
        let f0 = Field::new("f0", Int32, true);
        let f1 = Field::new("f1", UInt8, false);
        let f1_optional = Field::new("f1", UInt8, true);
        let f2 = Field::new("f2", String, true);

        let schema = Schema::new(vec![f0.clone(), f1.clone(), f2.clone()]);

        assert_eq!(schema.num_fields(), 3);
        assert_eq!(schema.field(0), Some(&f0));
        assert_eq!(schema.field(1), Some(&f1));
        assert_eq!(schema.field(2), Some(&f2));

        let schema2 = Schema::new(vec![f0.clone(), f1.clone(), f2.clone()]);
        let schema3 = Schema::new(vec![f0.clone(), f1_optional.clone(), f2.clone()]);

        assert!(schema == schema2);
        assert!(schema2 != schema3);
    }

    #[test]
    fn test_schema_to_string() {
        let f0 = Field::new("f0", Int32, true);
        let f1 = Field::new("f1", UInt8, false);
        let f2 = Field::new("f2", String, true);
        let f3 = Field::new("f3", List(Box::new(Int16)), true);

        let schema = Schema::new(vec![f0, f1, f2, f3]);
        let expected = r#"(f0: int32
 f1: uint8 not null
 f2: string
 f3: list<item: int16>)"#;

        assert_eq!(schema.to_string(), expected);
    }

    #[test]
    fn test_schema_get_field_by_name() {
        let f0 = Field::new("f0", Int32, true);
        let f1 = Field::new("f1", UInt8, false);
        let f2 = Field::new("f2", String, true);
        let f3 = Field::new("f3", List(Box::new(Int16)), true);

        let schema = Schema::new(vec![f0.clone(), f1.clone(), f2.clone(), f3.clone()]);

        let result = schema.get_field_by_name("f1");
        assert_eq!(result, Some(&f1));

        let result = schema.get_field_by_name("f3");
        assert_eq!(result, Some(&f3));

        let result = schema.get_field_by_name("not-found");
        assert_eq!(result, None);
    }


// TEST_F(TestSchema, GetFieldIndex) {
//   auto f0 = field("f0", int32());
//   auto f1 = field("f1", uint8(), false);
//   auto f2 = field("f2", utf8());
//   auto f3 = field("f3", list(int16()));

//   auto schema = ::arrow::schema({f0, f1, f2, f3});

//   ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
//   ASSERT_EQ(1, schema->GetFieldIndex(f1->name()));
//   ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
//   ASSERT_EQ(3, schema->GetFieldIndex(f3->name()));
//   ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
// }

// TEST_F(TestSchema, TestMetadataConstruction) {
//   auto f0 = field("f0", int32());
//   auto f1 = field("f1", uint8(), false);
//   auto f2 = field("f2", utf8());
//   auto metadata = std::shared_ptr<KeyValueMetadata>(
//       new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
//   auto schema = ::arrow::schema({f0, f1, f2}, metadata);
//   ASSERT_TRUE(metadata->Equals(*schema->metadata()));
// }

// TEST_F(TestSchema, TestAddMetadata) {
//   auto f0 = field("f0", int32());
//   auto f1 = field("f1", uint8(), false);
//   auto f2 = field("f2", utf8());
//   vector<shared_ptr<Field>> fields = {f0, f1, f2};
//   auto metadata = std::shared_ptr<KeyValueMetadata>(
//       new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
//   auto schema = std::make_shared<Schema>(fields);
//   std::shared_ptr<Schema> new_schema = schema->AddMetadata(metadata);
//   ASSERT_TRUE(metadata->Equals(*new_schema->metadata()));

//   // Not copied
// ASSERT_TRUE(metadata.get() == new_schema->metadata().get());

    // TODO macro

    #[test]
    fn test_datatype_name() {
        assert_eq!(Int8.name(), "int8");
        assert_eq!(Int16.name(), "int16");
        assert_eq!(Int32.name(), "int32");
        assert_eq!(Int64.name(), "int64");
        assert_eq!(UInt8.name(), "uint8");
        assert_eq!(UInt16.name(), "uint16");
        assert_eq!(UInt32.name(), "uint32");
        assert_eq!(UInt64.name(), "uint64");
    }


    #[test]
    fn test_primitive_types_to_string() {
        assert_eq!(Int8.to_string(), "int8");
        assert_eq!(Int16.to_string(), "int16");
        assert_eq!(Int32.to_string(), "int32");
        assert_eq!(Int64.to_string(), "int64");
        assert_eq!(UInt8.to_string(), "uint8");
        assert_eq!(UInt16.to_string(), "uint16");
        assert_eq!(UInt32.to_string(), "uint32");
        assert_eq!(UInt64.to_string(), "uint64");
    }


    #[test]
    fn test_sizeof() {

        //let e: () = ArrayMut { data: 3 };

        println!("{}", mem::size_of::<DataType>());
        //println!("{}", mem::size_of::<DataType::Int8>());
    }

}
