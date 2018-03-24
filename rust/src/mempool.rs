use std::heap::{Heap, Layout, Alloc};


type Mempool = Heap;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mempool_allocate() {
        let mut pool = Mempool::default();
        let layout = Layout::from_size_align(100, 1 << 6).unwrap();

        let excess = unsafe {
            pool.alloc_excess(layout).unwrap()
        };

        println!("{:?}", excess);
        // let mut pool = DefaultMemoryPool::new();
        // match pool.allocate(100) {
        //     Ok(page) => {
        //         assert_eq!(100, pool.bytes_allocated());
        //         assert_eq!(100, pool.max_memory());

        //         pool.free(page, 100);
        //         assert_eq!(0, pool.bytes_allocated());
        //         assert_eq!(100, pool.max_memory());
        //     },
        //     Err(e) => panic!("{}", e.message())
        // }
    }

}
