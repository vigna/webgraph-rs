/// A trait for an iterator on which the returned object can contain a reference
/// to the iterator itself. This implies that this kind of iterators cannot
/// be collected or used in parallel.
pub trait StreamingIterator {
    type StreamItem<'b>
    where
        Self: 'b;

    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>>;

    fn enumerate_stream(self) -> Enumerate<Self>
    where
        Self: Sized,
    {
        Enumerate {
            iter: self,
            count: 0,
        }
    }

    fn filter_stream<P>(self, predicate: P) -> Filter<Self, P>
    where
        Self: Sized,
        for<'a> P: FnMut(&Self::StreamItem<'a>) -> bool,
    {
        Filter {
            iter: self,
            predicate,
        }
    }

    fn map_stream<B, F>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        for<'a> F: FnMut(Self::StreamItem<'a>) -> B,
    {
        Map { iter: self, f }
    }

    fn zip_stream<I>(self, other_iter: I) -> Zip<Self, I>
    where
        Self: Sized,
    {
        Zip {
            iter: self,
            other_iter,
        }
    }

    fn for_each_stream<F>(mut self, mut f: F)
    where
        Self: Sized,
        for<'a> F: FnMut(Self::StreamItem<'a>),
    {
        while let Some(item) = self.next_stream() {
            f(item)
        }
    }
}

/// Blanket implementation for all the iterators which items do not contain
/// references to themselves. This is valid as owned objects have lifetime
/// can have for sure lifetime bigger than self.
impl<T> StreamingIterator for T
where
    T: Iterator,
{
    type StreamItem<'b> = T::Item where T: 'b;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        self.next()
    }
}

#[derive(Clone)]
pub struct Enumerate<I> {
    iter: I,
    count: usize,
}

impl<I> StreamingIterator for Enumerate<I>
where
    Self: Sized,
    I: StreamingIterator,
{
    type StreamItem<'b> = (usize, I::StreamItem<'b>) where Self: 'b;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        let res = self.iter.next_stream().map(|item| (self.count, item));
        self.count += 1;
        res
    }
}

#[derive(Clone)]
pub struct Map<I, F> {
    iter: I,
    f: F,
}

impl<I, F, B> StreamingIterator for Map<I, F>
where
    Self: Sized,
    I: StreamingIterator,
    F: for<'a> FnMut(I::StreamItem<'a>) -> B,
{
    type StreamItem<'b> = B where Self: 'b;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        self.iter.next_stream().map(&mut self.f)
    }
}

#[derive(Clone)]
pub struct Filter<I, P> {
    iter: I,
    predicate: P,
}

impl<I, P> StreamingIterator for Filter<I, P>
where
    Self: Sized,
    I: StreamingIterator,
    P: for<'a> FnMut(&I::StreamItem<'a>) -> bool,
{
    type StreamItem<'b> = I::StreamItem<'b> where Self: 'b;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        loop {
            let item = self.iter.next_stream()?;
            if (self.predicate)(&item) {
                return Some(item);
            }
        }
    }
}

#[derive(Clone)]
pub struct Zip<I, I2> {
    iter: I,
    other_iter: I2,
}

impl<I, I2> StreamingIterator for Zip<I, I2>
where
    Self: Sized,
    I: StreamingIterator,
    I2: StreamingIterator,
{
    type StreamItem<'b> = (I::StreamItem<'b>, I2::StreamItem<'b>) where Self: 'b;

    #[inline(always)]
    fn next_stream(&mut self) -> Option<Self::StreamItem<'_>> {
        self.iter.next_stream().zip(self.other_iter.next_stream())
    }
}

#[cfg(test)]
mod test_stream_iter {
    use super::*;

    #[test]
    fn test() {
        let truth = vec![1, 3, 5].into_iter();

        (0..10)
            .enumerate_stream()
            .filter_stream(|(_i, x)| x % 2 == 0)
            .map_stream(|(i, x)| x + i)
            .zip_stream(truth)
            .for_each_stream(|(x, truth)| {
                println!("{} == {} ?", x, truth);
                assert_eq!(x, truth);
            });
    }
}
