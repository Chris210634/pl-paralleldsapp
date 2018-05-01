#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <time.h>

using namespace std;

int worker_number = 0;
int number_of_workers = 2;
const long N = 4096000;
const int M = 100;
long array[N];
long stage_number = 1;
string barrier_file, data_file;

void routine();
void barrier();
void init();
void waitforworker(int i);
void read_data();
void write_data();

int main()
{
  init();
  for (int i = 0 ; i < M ; i++)
    {
    clock_t a = clock();
    routine();
    clock_t b = clock();
    write_data();
    clock_t c = clock();
    barrier();
    clock_t d = clock();
    read_data();
    clock_t e = clock();
    barrier();
    clock_t f = clock();
    cout << (double)(b-a)/CLOCKS_PER_SEC << ','
         << (double)(c-b)/CLOCKS_PER_SEC << ','
         << (double)(d-c)/CLOCKS_PER_SEC << ','
         << (double)(e-d)/CLOCKS_PER_SEC << ','
         << (double)(f-e)/CLOCKS_PER_SEC << endl;
    }
  return 0;
}

void routine()
{
  int p = 0;
  for ( long i = N*worker_number ; i < N*(worker_number + 1) ; i++)
    {
    array[p] = i;
    p++;
    }
}

void init()
{
  const char* env_worker_number = getenv("WORKER_NUMBER");
  const char* env_number_of_workers = getenv("NUMBER_OF_WORKERS");
  const char* env_tmp_path = getenv("TMP_PATH");
  worker_number = atoi(env_worker_number);
  number_of_workers = atoi(env_number_of_workers);
  barrier_file = string(env_tmp_path) + "/barrier";
  data_file = string(env_tmp_path) + "/data";
  /*if (worker_number == 0)
    {
    ofstream ofs;
    for( int i = 0 ; i < number_of_workers ; i++)
      {
      ofs.open("barrier"+to_string(i),ios::binary);
      ofs.write((char*)(&stage_number),sizeof(stage_number));
      ofs.close();
      }
    }*/
  cout << "I am " << worker_number << " of " << number_of_workers << endl;
  barrier();
}

void barrier()
{
  ofstream ofs;
  ofs.open(barrier_file+to_string(worker_number),ios::binary);
  ofs.write((char*)(&stage_number),sizeof(stage_number));
  ofs.close();
  for( int i = 0 ; i < number_of_workers ; i++)
    {
    if (i == worker_number) continue;
    waitforworker(i);
    }
  stage_number++;
}

void waitforworker(int i)
{
  long stage = 0;
  ifstream ifs;
  
  while (stage < stage_number)
    {
    ifs.open(barrier_file+to_string(i),ios::binary);
    ifs.read((char*)(&stage),sizeof(stage));
    ifs.close();
    }
}

void read_data()
{
  ifstream ifs;
  for( int i = 0 ; i < number_of_workers ; i++)
    {
    if (i == worker_number) continue;
    ifs.open(data_file + to_string(i),ios::binary);
    ifs.read((char*)(array),N*sizeof(long));
    assert(array[0] == N*i);
    ifs.close();
    }
}

void write_data()
{
  ofstream ofs;
  ofs.open(data_file+to_string(worker_number),ios::binary);
  ofs.write((char*)(array),N*sizeof(long));
  ofs.close();
}









