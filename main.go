// main
package main

import (
	"fmt"
	. "github.com/fanliao/go-plinq"
	"math"
)

type User struct {
	id   int
	name string
	age  int
	role *Role
}
type Role struct {
	name string
	id   int
}

func main() {
	fmt.Println("Hello plinq!")

	administrator := Role{"Administrator", 10}
	customer := Role{"Customer", 20}
	staff := Role{"Staff", 30}
	nothing := Role{"nothing", 40}

	henry := User{2, "Henry", 18, &customer}
	rock := User{1, "Rock", 20, &customer}
	jack := User{3, "Jack", 25, &administrator}
	stone := User{4, "Stone", 38, &staff}
	vic := User{5, "Vic", 22, &staff}

	roles := []*Role{&administrator, &customer, &staff, &nothing}
	users := []*User{&jack, &rock, &henry, &stone, &vic}

	_ = roles
	//1. From-Where-Select---------------------------------------
	isCustomer := func(u interface{}) bool {
		return u.(*User).role.name == "Customer"
	}
	getName := func(u interface{}) interface{} {
		return u.(*User).name
	}
	if names, err := From(users).Where(isCustomer).Select(getName).Results(); err == nil {
		fmt.Printf("%v\n", names)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	//2. Lazy executing------------------------------------------
	usersLazy := make([]*User, len(users))
	_ = copy(usersLazy, users)
	pUsers := &usersLazy
	q := From(pUsers).Where(isCustomer).Select(getName)

	*pUsers = append(*pUsers, &User{1, "Hal", 100, &customer})
	if names, err := q.Results(); err == nil {
		fmt.Printf("%v\n", names)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	//3. Channel as data source or output------------------------
	usersChan := func() chan *User {
		ch := make(chan *User)
		go func() {
			for _, v := range users {
				ch <- v
			}
			close(ch)
		}()
		return ch
	}
	q = From(usersChan()).Where(isCustomer).Select(getName)
	if names, err := q.Results(); err == nil {
		fmt.Printf("%v\n", names)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	q = From(usersChan()).Where(isCustomer).Select(getName)
	if namesChan, errChan, err := q.ToChan(); err == nil {
		rs := make([]interface{}, 0, 1)
		for v := range namesChan {
			rs = append(rs, v)
		}
		// rs is: {"Rock", "Henry"}

		if e, ok := <-errChan; ok {
			fmt.Println("get error:", e)
		}
		fmt.Printf("%v\n", rs)
		// return: {"Rock", "Henry"}
	}

	//4. GroupBy and Ordering-----------------------------------
	groupKeySelector := func(u interface{}) interface{} {
		return u.(*User).role.id
	}
	if groups, err := From(users).GroupBy(groupKeySelector).Results(); err == nil {
		// return: {10, [&jack]}, {20 , [&rock, &henry]}, {30 , [&stone, &vic]}
		for _, o := range groups {
			kv := o.(*KeyValue)
			fmt.Print("{", kv.Key, ",", kv.Value, "}")
		}
		fmt.Println()
	}

	//5. Order user by role id----------------------------------
	orderUserById := func(v1 interface{}, v2 interface{}) int {
		u1, u2 := v1.(*User), v2.(*User)
		if u1.id < u2.id {
			return -1
		} else if u1.id == u2.id {
			return 0
		} else {
			return 1
		}
	}
	if ordereds, err := From(users).OrderBy(orderUserById).Results(); err == nil {
		for _, v := range ordereds {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	//5. Aggregate operations----------------------------------
	userAge := func(u interface{}) interface{} {
		return u.(*User).age
	}
	if minAge, err := From(users).Select(userAge).Min(); err == nil {
		fmt.Printf("%v\n", minAge)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	minOpr := &AggregateOperation{Seed: math.MaxInt32,
		AggAction: func(v interface{}, t interface{}) interface{} {
			v1, t1 := v.(*User), t.(int)
			if v1.age < t1 {
				return v1.age
			} else {
				return t
			}
		},
		ReduceAction: func(t1 interface{}, t2 interface{}) interface{} {
			if t1.(int) < t2.(int) {
				return t1
			} else {
				return t2
			}
		}}
	if minAge, err := From(users).Aggregate(minOpr); err == nil {
		fmt.Printf("%v\n", minAge)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	if minAge, err := From(users).Select(userAge).Aggregate(Min, Max, Count); err == nil {
		fmt.Printf("%v\n", minAge)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	//6. Set Operations----------------------------------------------
	Users1 := []*User{&rock, &jack, &vic}
	Users2 := []*User{&henry, &stone, &vic, &vic}

	if bothUsers, err := From(Users1).Intersect(Users2).Results(); err == nil {
		for _, v := range bothUsers {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	if onlyUsers1, err := From(Users1).Except(Users2).Results(); err == nil {
		for _, v := range onlyUsers1 {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	if allUsers, err := From(Users1).Union(Users2).Results(); err == nil {
		for _, v := range allUsers {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	if users, err := From(Users2).Distinct().Results(); err == nil {
		for _, v := range users {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	//7. Joins-----------------------------------------------------------
	type namePair struct{ userName, roleName string }

	outerKeySel := func(v interface{}) interface{} { return v.(*Role).id }
	innerKeySel := func(v interface{}) interface{} { return v.(*User).role.id }
	resultSel := func(this, that interface{}) interface{} {
		return namePair{this.(*Role).name, that.(*User).name}
	}

	if pairs, err := From(roles).Join(users, outerKeySel, innerKeySel, resultSel).Results(); err == nil {
		fmt.Printf("%v\n", pairs)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	resultSel = func(this, that interface{}) interface{} {
		if that == nil {
			return namePair{this.(*Role).name, ""}
		} else {
			return namePair{this.(*Role).name, that.(*User).name}
		}
	}

	if pairs, err := From(roles).LeftJoin(users, outerKeySel, innerKeySel, resultSel).Results(); err == nil {
		fmt.Printf("%v\n", pairs)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	type pair struct {
		name        string
		countOfUser int
	}
	outerKeySel = func(v interface{}) interface{} { return v.(*Role).id }
	innerKeySel = func(v interface{}) interface{} { return v.(*User).role.id }
	resultSel1 := func(outer interface{}, inners []interface{}) interface{} {
		return pair{outer.(*Role).name, len(inners)}
	}

	if pairs, err := From(roles).GroupJoin(users, outerKeySel, innerKeySel, resultSel1).Results(); err == nil {
		fmt.Printf("%v\n", pairs)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

	if pairs, err := From(roles).LeftGroupJoin(users, outerKeySel, innerKeySel, resultSel1).Results(); err == nil {
		fmt.Printf("%v\n", pairs)
	} else {
		fmt.Printf("get error: %v\n", err)
	}

}
