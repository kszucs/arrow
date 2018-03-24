use alloc::raw_vec::RawVec;
use std::convert::From;
use std::ptr;

use dtypes::{DataType, PrimitiveType, ListType, List};


pub type Buffer<T> = RawVec<T>;
pub type BitMap = Buffer<bool>;


pub struct PrimitiveData<T: PrimitiveType> {
    len: usize,
    nulls: BitMap,
    values: Buffer<T::Item>
}


pub struct ListData<T: DataType> {
    len: usize,
    nulls: BitMap,
    offsets: Buffer<u32>,
    values: Array<T>
}


pub struct Array<T: DataType> {
    // atomic stuff etc.
    data: T::Data,
    dtype: T
}


pub trait Data<T: DataType> {

    fn empty(dtype: T) -> Self;

    fn len(&self) -> usize;
 
    fn push(&mut self, val: T::Item);

}


impl<T> Data<T> for PrimitiveData<T> where T: PrimitiveType {

    fn empty(dtype: T) -> Self {
        PrimitiveData { 
            len: 0,
            nulls: BitMap::new(),
            values: Buffer::new() 
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, val: T::Item) {    
        if self.len == self.values.cap() {
            self.values.double();
        }
        unsafe {
            ptr::write(self.values.ptr().offset(self.len as isize), val);
        }
        self.len += 1;
    }


}


impl<T> Data<List<T>> for ListData<T> where T: DataType {

    fn empty(dtype: List<T>) -> Self {
        ListData {
            len: 0,
            nulls: BitMap::new(),
            offsets: Buffer::new(),
            values: Array::new(dtype.0),
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, val: Vec<T::Item>) {
        unimplemented!()
    }
}


impl<T> Array<T> where T: DataType {

    fn new(dtype: T) -> Self {
        Array {
            data: T::Data::empty(dtype),
            dtype: dtype,
        }
    }

    pub fn dtype(&self) -> T { 
        self.dtype
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn push(&mut self, val: T::Item) {
        self.data.push(val)
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

        assert_eq!(a.data.len, 0);
        assert_eq!(a.dtype(), Int64);

        //println!("{}", a.len());
        println!("{}", a.data.values.cap());

        for i in 1..100 {
            a.push(i);
        }
    }

    #[test]
    fn test_list() {
        let mut a = Array::new(List(Int64));

        // a.push(vec![1,2,3]);
    }
}
