package orm

import "fmt"

func Example_bind_receivers() {

	type B struct {
		F3 string  `orm:"f3"`
		F4 *string `orm:"?f4"`
	}

	type A struct {
		F1 int     `orm:"f1"`
		F2 string  `orm:"f2"`
		B  B       `orm:"?"`
		F5 string  `orm:"?f5"`
		F6 float32 `orm:"?f6"`
		F7 string  `orm:"f7|f8"`
	}

	t := Table{
		Name: "t",
		columns: nameset{
			"f1": {},
			"f2": {},
			"f3": {},
			"f5": {},
			"f8": {},
		},
	}

	v := &A{}

	bb, err := t.bind_receivers(v)

	if err == nil {
		fmt.Printf("ss: %v\n", bb.selectors)
	}

	// Output:
	// ss: [f1 f2 f3 f5 f8]
}
