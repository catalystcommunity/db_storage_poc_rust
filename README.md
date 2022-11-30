# db_storage_poc_rust

This is purely a PoC of some concepts we believe will apply to a generalized database that we could use as an application database as well as an analytics platform. Specifically allowing more of an optimized approach to getting answers and exploring data than running arduous analytics or other data pipelines that just aren't needed, even on large datasets.

## Notes

In naive fashion, we can do a single u64 column at about 190 MB/s with very little memory usage. I'm sure this could be heavily optimized, but for a full table scan equivalent, this is very good speed for the PoC's success.

A more complicated set of queries with aggregation and whatnot, all in one scan of the data, is at a little over 25.2 MB/s

I am using a laptop with 64GB RAM and an M2 1TB SSD, this isn't meant to be that great or anything, just not in the way of the code doing what it can.

I believe there is a great deal of potential for a newcomer to the space of databases. My caveat to this is to avoid K/V stores. They seem to be in a lot of newer databases, especially analytics oriented databases (which I view as a step backwards). K/V is convenient to get started, but has severe issues as a low level architecture for things like distributed transactions or locking, or just working with page file style data management that's very useful for atomicity and the like. I think it's amazing that a bunch of databases have made it as far as they have with K/V being the underlying mechanism, but that easy start leaks very quickly when you start becoming a full fledged database.

Knowing what I know now from the PoC work, I would be looking at pages and cells as the underlying store format. I am very sold on the idea of an APL derivative for a query/explore platform for the data. It would be fantastic to build a tree of operations and get the free lineage of a simple program to all the result sets that might be needed. It's very conducive to the analyst, especially for building a simple UI off of for that analysis, and leaving the more complicated programs to the specialists.

I believe it would take a solid year or two before a full generalized database could be made in this generalized paradigm. It would deeply separate storage, compute, and a working query/explore paradigm for the kind of work you want to be doing, and I think the market for that is enormous, but even just competing with a Snowflake would be easy with this tool. It would be pennies on the dollar of comparative cost.

I also think there's some interesting but unproven work to be done in storage vs analysis. For instance, you could take the initial set of data you want to work with from the storage engine, and then keep all of that completely separate, maybe in a more ephemeral disk storage environment, and just fiddle with that without ever affecting the main database. It would be like an on-demand read-replica that you just make out of your laptop. Some potential to do this in-browser with edge-computing of some kind. I don't know the limits of WASM here, but I imagine storage is less of a concern as RAM usage.

Regardless, a successful PoC to be sure. The bottleneck is still the disk, but with some basic optimizations I think we could easily double or triple read speeds on full table scans and there's so much B-Tree work in the last 50 years that many things can be improved a great deal. This is a far cry from some of the commercial offerings in this space which still tout hours of processing as if that's a feature. This test of a billion orders failed miserably in several analytics DBs we have tried over the last few years, and this is a satisfying proving ground for our belief that they should have been better. They absolutely should have, and our naive code here shows just how off the mark they are.

## Rust Specifically

Rust is definitely a learning curve failure here. It's the right thing for the safety and typing needed, but I wouldn't do the query engine in it. This low level storage approach is showing what we can do without knowing what we're doing, so that's helpful. I'd be happy with the performance staying right here, let alone improving. It's already quite fast and very memory friendly, and with less naive work and a better file format (and in-memory representation) there's much to improve.

Rust also requires a very different kind of thinking. I should have gone through the Rust Book verbatim before touching this code base rather than winging it after learning the basics. The language is just not intuitive and very much works against you if you aren't conforming to the flows it makes easier.

## Try it Yourself

If you compile this with `cargo build --release` and then run it with the `generate` command, it will by default build 1 million orders with 100k customers and up to 10 products per order. Generation is pretty slow, much of which is generating random data and converting it to bytes to be written, all in pieces. It might be sped up with buffered file IO, but I have spent no time optimizing that since I run it once to do analysis many times.

The `analyze` command does full analysis on orders, order_products, and customers. The `average` command does analysis on a single column in order_products for the quantity column. These are naively implemented with some optimization attempts to get to reasonable speeds. It will output a bunch of findings, the bytes scanned, and a time scanned with bytes/second calculated for you.

Generate a large set of data with overrides, or small if you just want to try it out. It looks for data in the `demo_data` directory.
