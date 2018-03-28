use std::mem;
use std::fmt;
use std::ptr;
use alloc::raw_vec::RawVec;


// use type aliases!!!!

struct Boolean;
struct Int8;
struct Int16;
struct Int32;
struct Int64;
struct UInt8;
struct UInt16;
struct UInt32;
struct UInt64;
struct Float32;
struct Float64;

struct Decimal {
    precision: i32,
    scale: i32
}

// type String = String;
// type Binary = 
// FixedSizedBinary(i32),  // byte_width


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct List<T: DataType>(pub T);


struct Struct(Vec<(String, Box<DataType>)>);

// pub struct Struct(Vec<Box<DataType>>);


// every datatype mmust have an array type, nested types 
trait DataType {
    fn name(&self) -> &str;
    fn bits(&self) -> usize;
}


trait PrimitiveType: DataType {

}


trait ListType: DataType {
    
}

trait StructType: DataType {

}


// rename to numeric?
macro_rules! primitive {
    ($DT:ty, $T:ty, $name:expr) => (
        impl DataType for $DT {
            fn name(&self) -> &str {
                $name
            }

            fn bits(&self) -> usize {
                mem::size_of::<$T>() * 8
            }
        }

        impl PrimitiveType for $DT {

        }
        
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


impl<T: DataType> DataType for List<T> {

    fn name(&self) -> &str {
        "list"
    }

    fn bits(&self) -> usize {
        0
    }

}


//type Buffer<T> = RawVec<T>;

trait Buffer {
    
}

// impl<T> Buffer for RawVec<T> {

// }


struct Array {
    len: usize,
    dtype: Box<DataType>,
    buffers: Vec<Box<Buffer>>,
    children: Vec<Array>
}


trait Columnar<DT, T> {

    fn push(&mut self, value: T);

}


// impl Array {

//     fn empty<DT: DataType + 'static>(dtype: DT) -> Self {
//         Array {
//             len: 0,
//             dtype: Box::new(dtype),
//             buffers: Vec::new(),
//             children: Vec:: new()
//         }
//     }

// }


// impl Columnar<Int32, i32> for Array {

//     fn push(&mut self, value: i32) {
//         let mut buffer = self.buffers.get(0);
        
//         buffer.push(value);

//         // if self.len == self.data.0.cap() {
//         //     self.data.0.double();
//         // }
//         // unsafe {
//         //     let address = self.data.0.ptr().offset(self.len as isize);
//         //     ptr::write(address, value);
//         // }
//         // self.len += 1;
 
//     }

// }


// impl Columnar<Int64, i64> for Array {

//     fn push(&mut self, value: i64) {

//     }

// }


mod tests {
    use super::*;

    #[test]
    fn test_from_dtype() {

        let arr = Array::empty(Int32);
        let arr = Array::empty(Int64);


        // Array::new(Float32);
        // Array::new(Float64);

        // Array::new(Int8);
        // Array::new(Int16);
        // Array::new(Int32);
        // Array::new(Int64);
        // Array::new(UInt8);
        // Array::new(UInt16);
        // Array::new(UInt32);
        // Array::new(UInt64);

        // Array::new(List(Int64));
        // Array::new(List(Float64));
    }

    // #[test]
    // fn test_simple() {
    //     let mut a = Array::new(Int64);

    //     assert_eq!(a.data.len, 0);
    //     assert_eq!(a.dtype(), Int64);

    //     //println!("{}", a.len());
    //     println!("{}", a.data.values.cap());

    //     for i in 1..100 {
    //         a.push(i);
    //     }
    // }

    // #[test]
    // fn test_list() {
    //     let mut a = Array::new(List(Int64));

    //     a.push(vec![1, 2, 3]);
    //     a.push(vec![1, 2]);
    //     a.push(vec![4, 5, 6, 7, 8]);

    //     assert_eq!(a.len(), 3);
    //     assert_eq!(a.data.len(), 3);
    //     assert_eq!(a.data.values.len(), 10);
    // }

    // #[test]
    // fn test_nested_list() {
    //     let mut a = Array::new(List(List(Int64)));

    //     a.push(vec![vec![1, 2, 3], vec![3, 4, 5]]);
    //     a.push(vec![vec![1, 2, 3], vec![3, 5], vec![4, 6], vec![3]]);

    //     assert_eq!(a.len(), 2);
    //     assert_eq!(a.data.len(), 2);
    //     assert_eq!(a.data.values.len(), 6);
    //     assert_eq!(a.data.values.data.values.len(), 14);
    // }

}
