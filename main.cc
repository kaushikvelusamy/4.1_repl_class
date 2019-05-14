#include <string>
#include <tuple>
#include <vector>

#include <cilk.h>
#include <memoryweb.h>
#include <distributed.h>
extern "C" {
#include <emu_c_utils/layout.h>
}

typedef long Index_t;
typedef long Scalar_t;

/*
 * Overrides default new to always allocate replicated storage for instances
 * of this class. repl_new is intended to be used as a parent class for
 * distributed data structure types.
 */
class repl_new
{
public:
    // Overrides default new to always allocate replicated storage for
    // instances of this class
    static void *
    operator new(std::size_t sz)
    {
        return mw_mallocrepl(sz);
    }
    
    // Overrides default delete to safely free replicated storage
    static void
    operator delete(void * ptr)
    {
        mw_free(ptr);
    }
};

typedef std::vector<std::tuple<Index_t, Scalar_t>> Row_t;
typedef Row_t * pRow_t;
typedef pRow_t * ppRow_t;

class Matrix_t : public repl_new
{
public:
    static Matrix_t * create(Index_t nrows)
    {
        return new Matrix_t(nrows);
    }
    
    Matrix_t() = delete;
    Matrix_t(const Matrix_t &) = delete;
    Matrix_t & operator=(const Matrix_t &) = delete;
    Matrix_t(Matrix_t &&) = delete;
    Matrix_t & operator=(Matrix_t &&) = delete;
  
    // fake build function to watch migrations when adding rows
    // using replicated classes
    void build(Index_t row_idx)
    {
        Row_t tmpRow;
        if (row_idx % 2 == 0)
        {
            tmpRow.push_back(std::make_tuple(0,1));
            tmpRow.push_back(std::make_tuple(3,1));
            tmpRow.push_back(std::make_tuple(5,1));
            tmpRow.push_back(std::make_tuple(7,1));
            tmpRow.push_back(std::make_tuple(12,1));
            tmpRow.push_back(std::make_tuple(14,1));
            tmpRow.push_back(std::make_tuple(27,1));
            tmpRow.push_back(std::make_tuple(31,1));
        }
        else
        {
            tmpRow.push_back(std::make_tuple(1,1));
            tmpRow.push_back(std::make_tuple(7,1));
            tmpRow.push_back(std::make_tuple(10,1));
            tmpRow.push_back(std::make_tuple(14,1));
            tmpRow.push_back(std::make_tuple(18,1));
            tmpRow.push_back(std::make_tuple(27,1));
            tmpRow.push_back(std::make_tuple(28,1));
        }
        
        // bc of replication this does not cause migration
        pRow_t rowPtr = rows_[row_idx];
        
        for (Row_t::iterator it = tmpRow.begin(); it < tmpRow.end(); ++it)
        {
            rowPtr->push_back(*it);
        }
    }
    Index_t * nodelet_addr(Index_t i)
    {
        // dereferencing causes migrations
        return (Index_t *)(rows_ + i);
    }
  
private:
    Matrix_t(Index_t nrows) : nrows_(nrows)
    {
        nrows_per_nodelet_ = nrows_ + nrows_ % NODELETS();

        rows_ = (ppRow_t)mw_malloc2d(NODELETS(),
                                     nrows_per_nodelet_ * sizeof(Row_t));

        // replicate the class across nodelets
        for (Index_t i = 1; i < NODELETS(); ++i)
        {
            memcpy(mw_get_nth(this, i), mw_get_nth(this, 0), sizeof(*this));
        }

        // local mallocs on each nodelet
        for (Index_t i = 0; i < NODELETS(); ++i)
        {
            cilk_migrate_hint(rows_ + i);
            cilk_spawn allocateRow(i);
        }
        cilk_sync;
    }

    // localalloc a single row
    void allocateRow(Index_t i)
    {
        for (Index_t j = 0; j < nrows_per_nodelet_; ++j)
        {
            new(rows_[i] + j) Row_t();
        }
    }

    Index_t nrows_;
    Index_t nrows_per_nodelet_;
    ppRow_t rows_;
};

int main(int argc, char* argv[])
{
    starttiming();

#ifdef timeit
    double clockrate = 175.0;
    unsigned long nid = NODE_ID();
    unsigned long starttime = CLOCK();
#endif

    Index_t nrows = 16;

    // Matrix A will have 16 rows on each nodelet,total 16X8 rows
    Matrix_t * A = Matrix_t::create(nrows);
    // Matrix B will have 16 rows on each nodelet,total 16X8 rows
    Matrix_t * B = Matrix_t::create(nrows);

    Index_t nlet_idx_1 = 2;  // Build at 2nd nodelet [Nodelets start at 0 and end at 7]
    cilk_migrate_hint(A->nodelet_addr(nlet_idx_1));
    cilk_spawn A->build(nlet_idx_1);
    
    Index_t nlet_idx_2 = 6;  // Build at 6th nodelet
    cilk_migrate_hint(B->nodelet_addr(nlet_idx_2));
    cilk_spawn B->build(nlet_idx_2);
    
    cilk_sync;

#ifdef timeit
    unsigned long endtime = CLOCK();
    unsigned long nidend = NODE_ID();
    if (nid != nidend) printf("timing problem %lu %lu\n", nid, nidend);
    unsigned long totaltime = endtime - starttime;
    double ms = ((double) totaltime / clockrate) / 1000.0;
    printf("Clock %.1lf Mhz\t Total Cycles %lu\t Time(ms) %.1lf\n",
         clockrate, totaltime, ms); fflush(stdout);
#endif
    
    return 0;
}







