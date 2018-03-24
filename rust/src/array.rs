use alloc::raw_vec::RawVec;
use std::convert::From;
use std::ptr;

use dtypes::{DataType, PrimitiveType, ListType, List};


pub type Buffer<T> = RawVec<T>;
pub type BitMap = Buffer<bool>;


pub struct PrimitiveData<T: PrimitiveType>{
    values: Buffer<T::Item>
}


pub struct ListData<T: DataType> {
    offsets: Buffer<u32>,
    values: Array<T>
}


impl<T: PrimitiveType> PrimitiveData<T> {

    pub fn new() -> Self {
        PrimitiveData { values: Buffer::new() }
    }

}


impl<T: DataType> ListData<T> {

    pub fn new(dtype: T) -> Self {
        ListData {
            offsets: Buffer::new(),
            values: Array::new(dtype)
        }
    }
}


struct Array<T: DataType> {
    // atomic stuff etc.
    len: usize,
    dtype: T,
    nulls: BitMap,
    data: T::Data
}


impl<T> Array<T> where T: DataType + Copy {

    fn new(dtype: T) -> Self {
        let data = dtype.empty();
        Array {
            len: 0,
            dtype: dtype,
            nulls: BitMap::new(),
            data: data
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn dtype(&self) -> T { 
        self.dtype
    }

}


impl<T> Array<T> where T: PrimitiveType<Data=PrimitiveData<T>> {

    pub fn push(&mut self, val: T::Item) {    
        if self.len == self.data.values.cap() {
            self.data.values.double();
        }
        unsafe {
            ptr::write(self.data.values.ptr().offset(self.len as isize), val);
        }
        self.len += 1;
    }

}

// impl to_dtype static method for struct with arrow procedural macro


#[cfg(test)]
mod tests {
    use super::*;
    use dtypes::*;

    #[test]
    fn test_from_dtype() {
        Array::new(Float32);
        Array::new(Float64);

        Array::new(Int8);
        Array::new(Int16);
        Array::new(Int32);
        Array::new(Int64);
        Array::new(UInt8);
        Array::new(UInt16);
        Array::new(UInt32);
        Array::new(UInt64);

        Array::new(List(Int64));
        Array::new(List(Float64));
    }

    #[test]
    fn test_simple() {
        let mut a = Array::new(Int64);

        assert_eq!(a.len(), 0);
        assert_eq!(a.dtype(), Int64);

        println!("{}", a.len());
        println!("{}", a.data.values.cap());

        for i in 1..100 {
            a.push(i);
        }
    }

}
