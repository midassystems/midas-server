use std::cmp::Ord;

#[derive(Debug)]
pub struct MinHeap<T: Ord> {
    data: Vec<T>,
}

impl<T: Ord> MinHeap<T> {
    pub fn new() -> Self {
        MinHeap { data: Vec::new() }
    }
}

impl<T: Ord> MinHeap<T> {
    fn parent(&self, index: usize) -> Option<usize> {
        if index == 0 {
            None
        } else {
            Some((index - 1) / 2)
        }
    }

    fn left_child(&self, index: usize) -> Option<usize> {
        let left = (index * 2) + 1;

        if left < self.data.len() {
            Some(left)
        } else {
            None
        }
    }

    fn right_child(&self, index: usize) -> Option<usize> {
        let right = (index * 2) + 2;

        if right < self.data.len() {
            Some(right)
        } else {
            None
        }
    }

    pub fn push(&mut self, value: T) {
        self.data.push(value);
        self.bubble_up(self.data.len() - 1);
    }

    fn bubble_up(&mut self, mut index: usize) {
        while let Some(parent) = self.parent(index) {
            if self.data[index] < self.data[parent] {
                self.data.swap(parent, index);
                index = parent;
            } else {
                break;
            }
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.data.is_empty() {
            return None;
        }

        if self.data.len() == 1 {
            return self.data.pop();
        }

        let len = self.data.len();
        self.data.swap(0, len - 1);

        let value = self.data.pop();
        self.heapify_down(0);
        value
    }

    fn heapify_down(&mut self, mut index: usize) {
        loop {
            let left = self.left_child(index);
            let right = self.right_child(index);

            if let Some(left_index) = left {
                if self.data[left_index] < self.data[index] {
                    self.data.swap(index, left_index);
                    index = left_index;
                }
            } else if let Some(right_index) = right {
                if self.data[right_index] < self.data[index] {
                    self.data.swap(index, right_index);
                    index = right_index;
                }
            } else {
                break;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // pub fn peek(&self) -> Option<&T> {
    //     self.data.first()
    // }
    //
    // pub fn length(&self) -> usize {
    //     self.data.len()
    // }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let mut heap = MinHeap { data: Vec::new() };

        heap.push(10);
        heap.push(5);
        heap.push(20);
        heap.push(2);

        println!("Min: {:?}", heap.pop()); // Some(2)
        println!("Min: {:?}", heap.pop()); // Some(5)
        println!("Min: {:?}", heap.pop()); // Some(10)
        println!("{:?}", heap); // MinHeap { data: [2, 5, 20, 10] }
    }
}
