use std::mem;
use std::fmt;

use array::{PrimitiveData, ListData, BitMap};


//TODO: default implementations

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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Boolean;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Int8;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Int16;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Int32;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Int64;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct UInt8;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct UInt16;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct UInt32;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct UInt64;

// struct HalfFloat;
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Float32;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Float64;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Decimal {
    precision: i32,
    scale: i32
}

// type String = String;
// type Binary = 
// FixedSizedBinary(i32),  // byte_width

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Time32(TimeUnit);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Time64(TimeUnit);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Date32(DateUnit);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Date64(DateUnit);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct Interval(IntervalUnit);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct List<T: DataType>(pub T);


// every datatype mmust have an array type, nested types 
pub trait DataType : Copy {
    type Data;

    fn name(&self) -> &str;
    fn bits(&self) -> usize;
    fn empty(&self) -> Self::Data;            
}


pub trait PrimitiveType: DataType {
    type Item;
}


pub trait FloatingType: PrimitiveType {
    fn precision(&self) -> Precision;
}


pub trait ListType: DataType {

}

pub trait StructType: DataType {

}


// rename to numeric?
macro_rules! primitive {
    ($DT:ty, $T:ty, $name:expr) => (
        impl DataType for $DT {
            type Data = PrimitiveData<$DT>;

            fn empty(&self) -> Self::Data {
                Self::Data::new()
            }
            
            fn name(&self) -> &str {
                $name
            }

            fn bits(&self) -> usize {
                mem::size_of::<$T>() * 8
            }
        }

        impl PrimitiveType for $DT {
            type Item = $T;
        }

        // impl fmt::Display for $DT {

        //     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //         write!(f, "{}", self.name())
        //     }
            
        // }
        
    )
}


macro_rules! floating {
    ($DT:ty, $precision:expr) => (
        impl FloatingType for $DT {

            fn precision(&self) -> Precision {
                $precision
            }

        }
    )
}


primitive!(Int8,   i8, "int8");
primitive!(Int16, i16, "int16");
primitive!(Int32, i32, "int32");
primitive!(Int64, i64, "int64");

primitive!(UInt8,   u8, "uint8");
primitive!(UInt16, u16, "uint16");
primitive!(UInt32, u32, "uint32");
primitive!(UInt64, u64, "uint64");

primitive!(Float32, f32, "float32"); // Float
primitive!(Float64, f64, "float64"); // Double

floating!(Float32, Precision::Single);
floating!(Float64, Precision::Double);


impl<T: DataType + Copy> DataType for List<T> {
    type Data = ListData<T>;

    fn empty(&self) -> Self::Data {
        Self::Data::new(self.0)
    }
            
    fn name(&self) -> &str {
        "list"
    }

    fn bits(&self) -> usize {
        0
    }

}


impl<T: DataType + Copy> ListType for List<T> {

}
