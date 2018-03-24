#![feature(integer_atomics)]
#![feature(box_syntax, box_patterns)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(const_size_of)]
#![feature(allocator_api)]
#![feature(alloc)]

#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(clippy))]

extern crate libc;
extern crate alloc;

pub mod util;
pub mod types;
pub mod dtypes;
pub mod mempool;
pub mod array;


mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
