use alloc::raw_vec::RawVec;


#[derive(Copy, Clone, Debug)]
pub enum TimeUnit {
    Second,
    Milli,
    Micro,
    Nano
}

#[derive(Copy, Clone, Debug)]
pub enum DateUnit {
    Day,
    Milli
}

#[derive(Copy, Clone, Debug)]
pub enum IntervalUnit {
    YearMonth,
    DayTime
}


#[derive(Clone, Debug)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Date32(DateUnit),
    Date64(DateUnit),
    // Interval(IntervalUnit),
    List(Box<DataType>),
    Struct(Vec<(String, Box<DataType>)>)
}


// const Int64: DataType = DataType::Primitive(PrimitiveType::Int64);
// const UInt64: DataType = DataType::Primitive(PrimitiveType::Int64);


pub type Buffer<T> = RawVec<T>;
pub type BitMap = Buffer<bool>;


enum ArrayData {
    Int8(Buffer<i8>),
    Int16(Buffer<i16>),
    Int32(Buffer<i32>),
    Int64(Buffer<i64>),
    UInt8(Buffer<i8>),
    UInt16(Buffer<i16>),
    UInt32(Buffer<i32>),
    UInt64(Buffer<i64>),
    Float32(Buffer<f32>),
    Float64(Buffer<f64>),
    List(Buffer<usize>, Box<Array>),
    Struct(Vec<Array>)
}

struct Array {
    len: usize,
    mask: BitMap,
    data: ArrayData,
    dtype: DataType,
}

impl From<DataType> for ArrayData {

    fn from(dtype: DataType) -> Self {
        use self::ArrayData::*;
        match dtype {
            DataType::Int8  => Int8(Buffer::new()),
            DataType::Int16 => Int16(Buffer::new()),
            DataType::Int32 => Int32(Buffer::new()),
            DataType::Int64 => Int64(Buffer::new()),

            DataType::UInt8  => UInt8(Buffer::new()),
            DataType::UInt16 => UInt16(Buffer::new()),
            DataType::UInt32 => UInt32(Buffer::new()),
            DataType::UInt64 => UInt64(Buffer::new()),

            DataType::Float32 => Float32(Buffer::new()),
            DataType::Float64 => Float64(Buffer::new()),

            DataType::Time32(_) => Int32(Buffer::new()), 
            DataType::Time64(_) => Int64(Buffer::new()),

            DataType::Date32(_) => Int32(Buffer::new()),
            DataType::Date64(_) => Int64(Buffer::new()),

            DataType::List(box dtype) => {
                let inner = Box::new(Array::from(dtype));
                List(Buffer::new(), inner)
            },
            DataType::Struct(ref children) => {
                let inners = children.iter().cloned().map(
                    |(_, box dtype)| Array::from(dtype)
                );
                Struct(inners.collect())
            }
        }
    }
}


impl From<DataType> for Array {

    fn from(dtype: DataType) -> Self {        
        Self {
            len: 0,
            mask: BitMap::new(),
            data: ArrayData::from(dtype.clone()),
            dtype: dtype
        }
    }
}

// enum ArrayData<T> {
//     Primitive {
//         len: usize,
//         mask: BitMap,
//         values: Buffer<T>
//     },
//     List {
//         len: usize,
//         nulls: BitMap,
//         offsets: Buffer<usize>,
//         values: Box<Array<T>>
//     }
// }

// struct Array<T> {
//     dtype: DataType,
//     data: ArrayData<T>
// }


// trait PrimitiveArray<T> {

//     fn new() -> Self;

// }

// impl PrimitiveArray<i32> for Array<i32> {
    
//     fn new() -> Self {
//         let dtype = DataType::Primitive(PrimitiveType::Int32);
//         let data = ArrayData::Primitive {
//             len: 0,
//             mask: BitMap::new(),
//             values: Buffer::new()
//         };
//         Self { dtype, data }
//     }

// }

//use dtypes::*;


#[cfg(test)]
mod tests {
    use super::Array;
    use super::DataType::*;

    #[test]
    fn test_from_dtype() {
        let a = Array::from(Int64);
        let b = Array::from(List(Box::new(Int64)));
        //let c = Array::from(Box::new(Int64)));

        // let dtype = Int64;

        // let a = Array::from(dtype);
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
