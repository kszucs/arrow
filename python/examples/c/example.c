#include "lib_api.h"


int main(int argc, char *argv[]) {
	Py_Initialize();
	import_pyarrow__lib();
  // car.speed = atoi(argv[1]);
  // car.power = atof(argv[2]);
  // activate(&car);
	Py_Finalize();
}
