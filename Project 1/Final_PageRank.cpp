#include <iostream>
#include <bits/stdc++.h>
#include <iostream>
#include <mpi.h>
#include <string>
#include <cstdlib>
#include <sstream>
#include <ctime>

#define arr_size 99999999

using namespace std;
int main(int argc, char *argv[])
{
  
  clock_t total_time = clock();
  //MPI_Status status;
  //MPI_Request requestNull;
  
  //Initialize the MPI environment
  MPI_Init(&argc, &argv);
  // Find out rank, size
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); //proc id
  int num_of_tasks;
  MPI_Comm_size(MPI_COMM_WORLD, &num_of_tasks); // num of processes

  //printf("\nNumber of processes:%d\nProc ID:%d",num_of_processes,processID);

  char pfile_name[100]; //partition file name
  char gfile_name[100]; //graph file name
  FILE *partition_fp;
  FILE *graph_fp;
  
  int maxValue = 0; //will hold largest node val
  int fileSize;
  int* degree; //size = largest node
   
  int* edges; //edge info
  int *partition_Array; // partition

  int rounds = atoi(argv[3]);
  //int max_processes = atoi(argv[4]);
  int num_of_partitions = atoi(argv[4]);

  if(num_of_partitions != num_of_tasks){
    printf("Number of MPI Processes does not match number of partitions.");
    return 1;
  }
  //int number_of_lines = 0;
  char* buffer = (char*)malloc(sizeof(char)*1024*1024);

  /* Allocating INT_MAX size to all arrays */
  //degree = (int*)malloc((INT_MAX-1)*sizeof(int));
  //max size of int - 2,147,483,647
  degree = (int*)malloc(arr_size*sizeof(int));
  //memset(degree,0,(arr_size*sizeof(int)));
  
    //memset(credits,0,sizeof(double)*arr_size);

  partition_Array = (int*)malloc(arr_size*sizeof(int));
  //memset(partition,0,(arr_size*sizeof(int)));
  
  
  //reading the partition file
  /* Open file for reading*/
  strcpy(pfile_name,argv[2]); 
  //strcpy(pfile_name,"test_part2.txt");
  //printf("\npnamepfile name: %s",pfile_name);

  partition_fp = fopen(pfile_name,"r");

  if(partition_fp == NULL)
  {
    perror("fopen");
    exit(1);
  }
  fseek(partition_fp, 0L, SEEK_END);
  fileSize = ftell(partition_fp);

  /* Seek to the beginning of the file */
  fseek(partition_fp, 0L, SEEK_SET);

  /*Temporary character arrays for reading file*/
  char* nodeID = (char*)malloc(sizeof(char)*1024);
  char* nodeDegree = (char*)malloc(sizeof(char)*1024);
  char partitionID;

  int tab_flag1=0,tab_flag2=0;
  int tab_flag=0;
  int node_length = 0;
  int degree_length = 0;
  int int_node,int_degree,int_partition;
  int sizeofBuffer;
  int sizeofArrays;
  sizeofArrays = arr_size;
  //printf("\n  size of array:%d",sizeofArrays);
  clock_t tStart = clock();
  clock_t tRead = clock();
  //printf("\nreading file");

  while(!feof(partition_fp))
  {
    sizeofBuffer = fread(buffer, 1, 1024*1024, partition_fp);
    for(int i=0;i<sizeofBuffer;i++)
    {
      if(buffer[i]=='\n')
      {
        
        nodeID[node_length] = '\0';
        int_node = atoi(nodeID);
        if(maxValue < int_node)
          maxValue = int_node;
        
        if(maxValue > sizeofArrays)
        {
          //double the size
          degree = (int*)realloc(degree, (maxValue+1)*sizeof(int)*2);
          //credits = (double*)realloc(credits, (largest_node+1)*sizeof(double)*2);
          partition_Array = (int*)realloc(partition_Array, (maxValue+1)*sizeof(int)*2);
          sizeofArrays = (maxValue+1)*2;
        }
        
        nodeDegree[degree_length] = '\0';
        int_degree = atoi(nodeDegree);
        
        int_partition = 0 + (partitionID - '0'); //converting char to int
        
        /*storing node's degree in degree array with nodeID as index */
        degree[int_node-1] = int_degree; 
        partition_Array[int_node-1] = int_partition;
        //credits[int_node-1] = 1; //initially credits is 1
        
        node_length = 0;
        degree_length = 0;
        tab_flag = 0;   
      }
      else if(buffer[i]==' ' || buffer[i]=='\t')
      {
        tab_flag++;
      }   
      else if(tab_flag == 0) //nodeID
      {       
        //printf("\nreading nodeID\nno tab reached");
        nodeID[node_length] = buffer[i]; //appending all characters to nodeID char array
        node_length++;
      }     
      else if(tab_flag == 1)
      {
        nodeDegree[degree_length] = buffer[i];
        degree_length++;
      }
      else if(tab_flag==2)
      {
        partitionID = buffer[i];
      } 
    }
  }
  fclose(partition_fp);
  //printf("\nTime taken to read the partition file , partition %d = %.2fs\n",rank,(double)(clock() - tStart)/CLOCKS_PER_SEC);
  //printf("\nLargest node:%d\n",largest_node);
  
  if(maxValue < sizeofArrays)
  {
    //printf("reallocating array size");
    degree = (int*)realloc(degree, (maxValue+1)*sizeof(int));
    //credits = (double*)realloc(credits, ((largest_node+1))*sizeof(double));
    partition_Array = (int*)realloc(partition_Array, (maxValue+1)*sizeof(int));
  }
  
  //printf("\n***after relloc***\n");
  
  

  

  strcpy(gfile_name,argv[1]);
  
  graph_fp = fopen(gfile_name,"r");

  if (graph_fp == NULL)
  {
    perror("fopen");
    exit(1);
  }
  fseek(graph_fp, 0L, SEEK_END);
  fileSize = ftell(graph_fp);
  // Seek to the beginning of the file 
  fseek(graph_fp, 0L, SEEK_SET);

  edges = (int*)malloc(sizeof(int)*arr_size);
  int edge_arr_size = arr_size;
  
  int edgeline_no;
  int num_of_lines = 0;
  int node1_length = 0;
  int node2_length = 0;
  int n1,n2;
  char* node1 = (char*)malloc(sizeof(char)*1024);
  char* node2 = (char*)malloc(sizeof(char)*1024);

  sizeofBuffer=0;
  tab_flag = 0;
  edgeline_no = 0;
  tStart = clock();

  while(!feof(graph_fp))
  {
    sizeofBuffer = fread(buffer, 1, 1024*1024, graph_fp);
    for(int i=0;i<sizeofBuffer;i++)
    {
      if(buffer[i]=='\n')
      {

        tab_flag = 0;
        node1[node1_length] = '\0'; 
        node2[node2_length] = '\0';
        n1 = atoi(node1);
        n2 = atoi(node2);

        //if(partition[n1]==processID) //n1 is an internal node
        //{ 
          //printf("\nedge ----n1 is internal\n %d\t%d",n1,n2);
          edges[edgeline_no] = n1; //assigning nodes to the edge array
          edges[edgeline_no+1]=n2;
          
          //printf("\n%d\t%d",edges[edge_index],edges[edge_index+1]);
          edgeline_no = edgeline_no + 2;
          num_of_lines++;
          if(num_of_lines > edge_arr_size)
        {
          //double the size
          edges = (int*)realloc(edges, (num_of_lines+1)*sizeof(int)*2);
          //credits = (double*)realloc(credits, (largest_node+1)*sizeof(double)*2);
          edge_arr_size = (num_of_lines+1)*2;
        }
        //}
        /*else if(partition[n2]==processID && partition[n1]!=processID) //n2 is an internal node and n1 - external
        {
          //printf("\nedge ----n2 is internal\n %d\t%d",n2,n1);
          edges[edgeline_no] = n2; //assigning nodes to the edge array
          edges[edgeline_no+1]=n1;
          
          //printf("\n%d\t%d",edges[edge_index],edges[edge_index+1]);
          edgeline_no = edgeline_no + 2;
          num_of_lines++;
        }*/
        node1_length = 0;
        node2_length = 0;
      }
      else if(buffer[i]==' ' || buffer[i]=='\t')
      {
        tab_flag = 1;
      }
      else if(tab_flag==0)
      {           
        node1[node1_length] = buffer[i]; //appending all characters to node1 char array
        node1_length++;
      }       
      else if(tab_flag==1)
      {
        node2[node2_length] = buffer[i];
        node2_length++;
      }
    }
  }
  fclose(graph_fp);
  
  //printf("\n\nTime taken to read the graph file, partition %d = %.2fs\n",rank,(double)(clock() - tStart)/CLOCKS_PER_SEC);
  printf("\n\nTime taken to read the the input files, partition %d = %.2fs\n",rank,(double)(clock() - tRead)/CLOCKS_PER_SEC);
  
  //cout<<"maxValue"<< maxValue<<"\n";
  /*for(int i=0;i<num_of_lines*2;i++){
    cout<<"edges"<<" "<<i<< array[i]<<"\n";
  }
    for(int i=0;i<=maxValue;i++){
    cout<<"degree"<<" "<<i<< degree[i]<<"\n";
  }
  for(int i=0;i<=maxValue;i++){
    cout<<"partArray"<<" "<<i<< partArray[i]<<"\n";
  }*/




  double *credit;
  credit = (double *) malloc((maxValue) * sizeof(double ));
  for (int i = 0; i <= maxValue; i++) {
    credit[i] = 1;
  }
  
  for (int n = 0 ; n < rounds ; n++) {
    tStart = clock();
    double *buff_send;
    buff_send = (double *) malloc((maxValue) * sizeof(double ));
    for (int i = 0; i <= maxValue; i++) {
      buff_send[i] = 0;
    }
    double *buff_recv;
    buff_recv = (double *) malloc((maxValue) * sizeof(double ));
    for (int i = 0; i <= maxValue; i++) {
      buff_recv[i] = 0;
    }

    for (int i = 0; i < num_of_lines*2; i = i + 2) {
      int f = edges[i];
      int j = i + 1;
      int s = edges[j];
      /*if ( f == 5) {
        cout << "\n5: \t" << s << "\t" << partArray[f - 1] << "\t " << credit[s - 1] / degree[s - 1] << "\n";
      }
      if ( f == 1) {
        cout << "\n1: \t" << s << "\t" << partArray[f - 1] << "\t " << credit[s - 1] / degree[s - 1] << "\n";
      }*/
      if ( partition_Array[f - 1] == rank) {
        buff_send[f - 1] += credit[s - 1] / degree[s - 1];
        buff_send[s - 1] += credit[f - 1] / degree[f - 1];
      }
    }
    
MPI_Allreduce(buff_send, buff_recv, maxValue , MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

    credit = buff_recv;
    printf("\nTime taken for round %d, partition %d = %.2fs\n",n+1,rank,(double)(clock() - tStart)/CLOCKS_PER_SEC);
    ofstream output;
    stringstream fn;
    fn << "Round" << n+1 << ".txt";
    output.open(fn.str());
    tStart = clock();


    for (int i = 0; i <= maxValue; i++) {
      output << i + 1 << "\t" << degree[i] << "\t";
      output << credit[i] << "\t";
      output << "\n";
    }


  }
      printf("\nTime taken to write for round %d = %.2fs\n",rank,(double)(clock() - tStart)/CLOCKS_PER_SEC);
  printf("\n Total Time taken by partition %d = %.2fs\n",rank,(double)(clock() - total_time)/CLOCKS_PER_SEC);
  //read_duration = (clock() - start_clock) / (double) CLOCKS_PER_SEC;
  //printf("\nTime to read was %f \n", read_duration);
  //cout << "\nDone writing\n";
  MPI_Finalize();
  return 0;
}
