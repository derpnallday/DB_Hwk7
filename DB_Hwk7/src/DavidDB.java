import exceptions.*;
import perf.Timeable;
import solver.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * DavidDB: The finest relational database system written in the common era.
 *
 * @author David
 * @version 6/25/18
 */
public class DavidDB extends AbstractDB implements Timeable{

	protected double time;
	protected double startTime;
	protected double endTime;

	/**
	 * Creates a new instance of DavidDB.
	 * @param filename	path to the schema file.
	 */
	public DavidDB(String filename) throws FileNotFoundException {
		super(filename);
		this.time = 0;
		this.startTime = 0;
		this.endTime = 0;
	}

	/**
	 * Creates (but does not populate) the relations specified in the schema file.
	 * @throws FileNotFoundException if the schema file does not exist
	 * @throws DBException if the an unrecognized data type is detected
	 */
	@Override
	public void createRelations() throws FileNotFoundException {
		BufferedReader fin = new BufferedReader(new FileReader(this.schema_file));
		String line;
		try {
			// each line contains: rel(type1 a1, type2 a2,...)
			while ((line = fin.readLine()) != null) {
				if (line.matches("^.+\\((.,?)+\\)$")) {
					// get relation name and instantiate relation
					String rel_name = line.substring(0, line.indexOf("("));

					// instantiate new relation
					Relation newRelation = new Relation(rel_name);

					// build attribute list
					List<Attribute> list = new ArrayList<>();
					String[] attr_token = line.substring(line.indexOf("(")).
							replaceAll("[()]","").split(",");

					// each attr_token is: "ATTRIBUTE_NAME TYPE"
					for (String attr_str : attr_token) {
						String[] pair = attr_str.trim().split("\\s+");
						pair[1] = pair[1].toUpperCase();
						switch (pair[1]) {
						case "TEXT":
							list.add(new Attribute(newRelation, Attribute.Type.TEXT, pair[0]));
							break;
						case "NUMERIC":
							list.add(new Attribute(newRelation, Attribute.Type.NUMERIC, pair[0]));
							break;
						default:
							throw new DBException("Unrecognized data type for attribute " + pair[0] +
									": " + pair[1]);
						}
					}
					// add attributes
					newRelation.setAttributes(list);

					// instantiate the relation
					this.relations.put(rel_name, newRelation);
				}
			}
			fin.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets a reference to the stored relation with the given name
	 * @param name	the name of the relation (case sensitive)
	 * @return	relation with the given name, or null if not exists
	 */
	@Override
	public AbstractRelation getRelation(String name) {
		return relations.getOrDefault(name, null);
	}

	/**
	 * Generates and returns a string containing all the relations defined
	 * in this database in no particular order.
	 *
	 * @return string containing R1(a1,..) followed by R2(a1,..), etc.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Collection<AbstractRelation> relations = this.relations.values();
		for (AbstractRelation r : relations) {
			sb.append(r.schemaToString()).append("\n");
		}
		return sb.toString();
	}

	/**
	 * (Hwk 2 addition)
	 * Performs intersection between two relations
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the intersection
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation intersect(Relation first, Relation second) throws DBException {
		return this.minus(first, this.minus(first,second));
	}

	/**
	 * (Hwk 2 addition)
	 * Performs union between this and the given relation
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the union
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation union(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}
		if (!isCompatible(first, second)) {
			throw new DBException("Union: incompatible relations " +
					first.schemaToString() + " and " + second.schemaToString());
		}	

		startTime = System.nanoTime();		
		Relation new_relation = (Relation) first.clone();
		new_relation.getTuples().addAll(second.getTuples());
		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return new_relation;	

	}

	/**
	 * (Hwk 2 addition)
	 * Performs a set difference between this and the given relation
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the difference
	 * @throws DBException if relations are incompatible
	 */
	@Override
	public Relation minus(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}
		if (!isCompatible(first, second)) {
			throw new DBException("Set difference: incompatible relations" +
					first.schemaToString() + " and " + second.schemaToString());
		}


		//get time
		startTime = System.nanoTime();	

		// generate a new relation containing the diff
		Relation new_relation = (Relation) first.clone();
		new_relation.getTuples().removeAll(second.getTuples());

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();
		return new_relation;
	}

	/**
	 * (Hwk 2 addition)
	 * Performs a cartesian product between two relations
	 * @param first		one relation
	 * @param second	second relation
	 * @return a relation containing the cartesian product, or null if either is null
	 */
	@Override
	public Relation times(Relation first, Relation second) throws DBException {
		if (first == null || second == null) {
			return null;
		}


		//get time
		startTime = System.nanoTime();	

		Relation left = (Relation) first.clone();
		Relation right = (Relation) second.clone();
		Relation new_relation = new Relation();

		List<Attribute> new_attr = left.getAttributes();
		new_attr.addAll(right.getAttributes());
		new_relation.setAttributes(new_attr);

		Set<Tuple> firstTuples = left.getTuples();
		Set<Tuple> secondTuples = right.getTuples();
		for (Tuple tuple : firstTuples) {
			//concatenate current tuple with another tuple from second table
			for (Tuple other_tuple : secondTuples) {
				Tuple new_tuple = tuple.concat(other_tuple);
				new_tuple.setRelation(new_relation);
				new_relation.addTuple(new_tuple);
			}
		}

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return new_relation;
	}

	/**
	 * (Hwk 2 addition)
	 * Determines whether two relations are compatible for set operations
	 * @param first		one relation
	 * @param second	second relation
	 * @return true if compatible, false otherwise
	 */
	private boolean isCompatible(Relation first, Relation second) {
		List<Attribute> first_list = first.getAttributes();
		List<Attribute> second_list = second.getAttributes();
		if (first_list.size() == second_list.size()) {
			for (int i = 0; i < first_list.size(); i++) {
				if (!first_list.get(i).getType().equals(second_list.get(i).getType())) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * (Hwk3 addition)
	 * Evaluates the specified condition on the current relation
	 * @param r	the relation to perform the selection
	 * @param cond_str	a boolean condition
	 * @return a reference to a relation which stores only the tuples
	 *          for which the condition evaluated true
	 * @throws DBException if the given condition is invalid
	 */
	@Override
	public Relation select(Relation r, String cond_str) throws DBException {
		if (cond_str == null || cond_str.equals("")) {
			return r;
		}

		//get time
		startTime = System.nanoTime();

		Relation result = (Relation) r.clone();
		result.getTuples().clear();

		Set<Tuple> set = r.getTuples();
		for (Tuple candidate : set) {
			if (this.evaluate(r, candidate, cond_str)) {
				result.addTuple(candidate);
			}
		}

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return result;
	}

	/**
	 * (Hwk3 addition)
	 * Evaluates a boolean expression given a tuple and its relation
	 * @param r	reference to a relation
	 * @param t reference to a tuple
	 * @param cond_str	a boolean condition
	 * @return true if condition is true for the given tuple; false otherwise.
	 * @throws DBException if the given condition is invalid
	 */
	private boolean evaluate(Relation r, Tuple t, String cond_str) throws DBException {
		// split up the condition into tokens
		String[] tokens = cond_str.split("\\s+");

		// run through each expression token and replace a variable with a value (if exists)
		for (int i = 0; i < tokens.length; i++) {
			try {
				tokens[i] = t.valueOf(tokens[i]).toString();
			} catch(DBException e) {
				// ignore
			}
		}
		//re-build the conditional
		StringBuilder sb = new StringBuilder();
		for (String tok : tokens) {
			sb.append(tok + " ");
		}
		return Expression.eval(sb.toString());
	}

	/**
	 * (Hwk3 addition)
	 * This method accepts a list of Attributes, and retains only the values
	 * pertaining to those attributes, for each tuple.
	 * @param r	the relation to perform the project
	 * @param projection_list	an array of attribute names (i.e., "A" or "R.A") to project
	 * @return a reference to a relation with the projected attributes, or null
	 * 			if no attributes are given.
	 * @throws DBException if an attribute name doesn't exist or is ambiguous
	 */
	public Relation project(Relation r, String[] projection_list) throws DBException {
		//get time
		startTime = System.nanoTime();

		// get attributes of r
		List<Attribute> attributes = r.getAttributes();

		// build attribute list of the project, and also check for ambiguity
		List<Attribute> list = new ArrayList<>();
		for (String attr_name : projection_list) {
			list.add(attributes.get(r.lookup(attr_name)));
		}

		//build new relation
		Relation projection = new Relation();
		projection.setAttributes(list);
		for (Tuple t : r.getTuples()) {
			//build up a tuple; examine and preserve only the given attributes
			List<Comparable> new_values = new ArrayList<>();
			for (int i = 0; i < list.size(); i++) {
				new_values.add(t.valueOf(list.get(i).getPedanticName()));
			}
			Tuple new_tuple = new Tuple(new_values, projection);
			projection.addTuple(new_tuple);
		}

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return projection;
	}

	/**
	 * (Hwk3 addition)
	 * Performs a natural join between two relations.
	 * @param r1	first relation
	 * @param r2	second relation
	 * @return a reference to a relation containing the joined data
	 */
	@Override
	public Relation naturalJoin(Relation r1, Relation r2) throws DBException {
		// determine common attributes
		Set<Attribute> common = new HashSet<>(r1.getAttributes());
		common.retainAll(r2.getAttributes());
		if (common.size() == 0) {	// no common attributes, natural join reduces to product
			return this.times(r1,r2);
		}

		// build expression to enforce equality
		StringBuilder expr = new StringBuilder();
		int count = 0;
		for (Attribute c : common) {
			if (count > 0) {
				expr.append(" && ");
			}
			expr.append(r1.getName() + "." + c.getName() + " = " + r2.getName() + "." + c.getName());
			count++;
		}
		// build projected attributes in order of: r1.a1, ..., r2.a1, ...
		List<Attribute> project_attr = new ArrayList<>(r1.getAttributes());
		for (Attribute a : r2.getAttributes()) {
			if (!project_attr.contains(a)) {
				project_attr.add(a);
			}
		}
		String[] project_array = new String[project_attr.size()];
		for (int i = 0; i < project_attr.size(); i++) {
			project_array[i] = project_attr.get(i).getPedanticName();
		}
		return this.project(this.select(this.times(r1,r2), expr.toString()), project_array);
	}


	/**
	 * (Hwk3 addition)
	 * Renames the given relation.
	 * @param r	the relation to rename
	 * @param newName	a new name
	 * @return the given relation after renaming
	 */
	@Override
	public Relation renameRelation(Relation r, String newName) {
		//get time
		startTime = System.nanoTime();

		Relation new_relation = (Relation) r.clone();

		// reset the attributes' relation
		List<Attribute> list = new_relation.getAttributes();
		for (Attribute a : list) {
			a.setRelation(new_relation);
		}
		new_relation.setName(newName);
		new_relation.setAttributes(list);	// this to reset the map with new pedantic names

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return new_relation;
	}

	/**
	 * (Hwk3 addition)
	 * Renames the list of attributes. Takes short names (not pedantic ones).
	 * @param r the relation whose attributes we want to rename
	 * @param list	a list of new attribute names
	 * @return the given relation after renaming
	 * @throws DBException if list size differs from number of attributes
	 */
	@Override
	public Relation renameAttributes(Relation r, String[] list) throws DBException {
		//get time
		startTime = System.nanoTime();


		Relation new_relation = (Relation) r.clone();

		// reset the attributes' relation
		List<Attribute> new_list = new_relation.getAttributes();

		// ensure same number of attributes are given
		List<Attribute> attribute_list = new_relation.getAttributes();
		if (attribute_list.size() != list.length) {
			throw new DBException("Attribute size mismatch. Required: " +
					attribute_list.size() + " attributes.");
		}

		// rename each attribute, and reset their relation reference
		for (int i = 0; i < list.length; i++) {
			new_list.get(i).setName(list[i]);
			new_list.get(i).setRelation(r);
		}
		new_relation.setAttributes(new_list);

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return new_relation;
	}

	/**
	 * (Hwk 5 addition)
	 * Aggregates, possibly over group(s), across the given relation
	 * @param r			relation over which to aggregate
	 * @param agg_fns	a list of aggregation functions (see: Agg enum)
	 * @param attrs		names of the attribute to apply the aggregation function
	 * @return	a relation containing the group(s) and aggregated value
	 * @throws DBException if attribute name or any groups are unknown, or if aggregation function cannot be performed
	 */
	@Override
	public Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs)
			throws DBException {
		return this.aggregate(r, agg_fns, attrs, null);
	}

	/**
	 * (Hwk 5 addition)
	 * Aggregates, possibly over group(s), across the given relation
	 * @param r			relation over which to aggregate
	 * @param agg_fns	a list of aggregation functions (see: Agg enum)
	 * @param attrs		names of the attribute to apply the aggregation function
	 * @param groups	a list of groups, or null if no groups
	 * @return	a relation containing the group(s) and aggregated value
	 * @throws DBException if attribute name or any groups are unknown, or if aggregation function cannot be performed
	 */
	@Override
	public Relation aggregate(Relation r, Agg[] agg_fns, String[] attrs, String[] groups)throws DBException {
		//get time
		startTime = System.nanoTime();

		/*** phase 1: create new relation and specify its attributes ***/
		Relation new_relation = new Relation();
		List<Attribute> attr_list = new ArrayList<>();

		// group is specified -- add those attribute(s) first
		if (groups != null) {
			for (int i = 0; i < groups.length; i++) {
				int group_pos = r.lookup(groups[i]);
				attr_list.add(new Attribute(new_relation, r.getAttributes().get(group_pos).getType(), groups[i]));
			}
		}

		// create the aggregate attribute(s)
		for (int i = 0; i < attrs.length; i++) {
			int agg_pos = r.lookup(attrs[i]);
			Attribute.Type type = (agg_fns[i].equals(Agg.COUNT)) ?
					Attribute.Type.NUMERIC : r.getAttributes().get(agg_pos).getType();
			attr_list.add(new Attribute(new_relation, type, agg_fns[i] + "(" + attrs[i] + ")"));
		}
		// add relevant attributes to the relation
		new_relation.setAttributes(attr_list);

		/*** phase 2: perform aggregation ***/
		if (agg_fns == null || agg_fns.length == 0) {
			throw new DBException("No aggregation function specified.");
		}

		// sort the data into groups; each inner list is a group. Just one group if no groups were selected
		List<List<Tuple>> all_groups = groupIt(r, groups);
		for (List<Tuple> group : all_groups) {
			// aggregation tuple
			Tuple agg_tuple = new Tuple(new Comparable[]{}, new_relation);

			// concatenate grouping attributes (any tuple will do)
			if (groups != null) {
				for (int i = 0; i < groups.length; i++) {
					agg_tuple = agg_tuple.concat(new Tuple(new Comparable[]{group.get(0).valueOf(groups[i])}, new_relation));
				}
			}

			// perform the aggregation function and obtain aggregated value
			Comparable[] agg_values = new Comparable[attrs.length];    // this will store the aggregated values
			for (int i = 0; i < attrs.length; i++) {
				switch (agg_fns[i]) {
				case SUM:
				case SUM_DISTINCT:
					if (r.getAttributes().get(r.lookup(attrs[i])).getType() == Attribute.Type.TEXT) {
						throw new DBException("Type mismatch: Cannot perform SUM() over TEXT attribute: " + attrs[i]);
					}
					agg_values[i] = (agg_fns[i] == Agg.SUM) ?
							this.agg_fn_sum(group, attrs[i], false) :
								this.agg_fn_sum(group, attrs[i], true);
							break;
				case AVG:
				case AVG_DISTINCT:
					if (r.getAttributes().get(r.lookup(attrs[i])).getType() == Attribute.Type.TEXT) {
						throw new DBException("Type mismatch: Cannot perform AVG() over TEXT attribute: " + attrs[i]);
					}
					agg_values[i] = (agg_fns[i] == Agg.AVG) ?
							this.agg_fn_avg(group, attrs[i], false) :
								this.agg_fn_avg(group, attrs[i], true);
							break;
				case COUNT:
					agg_values[i] = this.agg_fn_cnt(group, attrs[i], false);
					break;
				case COUNT_DISTINCT:
					agg_values[i] = this.agg_fn_cnt(group, attrs[i], true);
					break;
				case MAX:
					agg_values[i] = this.agg_fn_max(group, attrs[i], false);
					break;
				case MIN:
					agg_values[i] = this.agg_fn_min(group, attrs[i], false);
					break;
				default:
					throw new DBException("Unknown aggregation function: " + agg_fns[i]);
				}
			}
			new_relation.addTuple(agg_tuple.concat(new Tuple(agg_values, new_relation)));
		}

		endTime = System.nanoTime();

		//add time
		time += endTime - startTime;
		resetTimers();

		return new_relation;
	}

	/**
	 * (Hwk 5 -- not given)
	 * Creates a list of groups. Each group is a list containing tuples that belong to it.
	 * @param r	relation of the groups
	 * @param groups an array of group names
	 * @return list of groups. If groups is null, return one group with all tuples
	 */
	private List<List<Tuple>> groupIt(Relation r, String[] groups) {
		List<List<Tuple>> my_groups = new ArrayList<>();

		// sort tuples by g1, g2, g3, ...
		List<Tuple> sortedTuples = new ArrayList<>(r.getTuples());
		List<Tuple> group;
		if (groups != null) {
			GroupComparator grp_cmp = new GroupComparator(r, groups);
			Collections.sort(sortedTuples, grp_cmp);

			// like map-reduce
			group = new ArrayList<>();
			group.add(sortedTuples.get(0));
			for (int i = 1; i < sortedTuples.size(); i++) {
				if (grp_cmp.compare(sortedTuples.get(i - 1), sortedTuples.get(i)) == 0) {
					group.add(sortedTuples.get(i));
				} else {
					// new group found; cut off old group, create new one
					my_groups.add(group);
					group = new ArrayList<>();
					group.add(sortedTuples.get(i));
				}
			}
		}
		else {
			group = sortedTuples;
		}
		my_groups.add(group);
		return my_groups;
	}

	/**
	 * (Hwk 5 -- not given)
	 * Produces the number of tuples
	 * @param tuples	set of tuples
	 * @param agg_attr	name of the aggregating attribute
	 * @param distinct	whether to ignore duplicate values
	 * @return the number of tuples
	 */
	private Double agg_fn_cnt(Collection<Tuple> tuples, String agg_attr, boolean distinct) {
		if (!distinct) {
			return (double) tuples.size();
		}

		Set<Comparable> dupes = new HashSet<>();	// for distinct
		for (Tuple t : tuples) {
			dupes.add(t.valueOf(agg_attr));	// get the value to aggregate from the tuple
		}
		return (double) dupes.size();
	}


	/**
	 * (Hwk 5 -- not given)
	 * Produces the sum of the set of tuples for the given attribute position
	 * @param tuples	set of tuples
	 * @param agg_attr	name of the aggregating attribute
	 * @param distinct	whether to ignore duplicate values
	 * @return sum of the attribute
	 */
	private Double agg_fn_sum(Collection<Tuple> tuples, String agg_attr, boolean distinct) {
		Set<Comparable> dupes = new HashSet<>();	// for distinct

		double sum = 0.0;
		Comparable val;
		for (Tuple t : tuples) {
			val = t.valueOf(agg_attr);	// get the value to aggregate from the tuple

			if (distinct) {
				if (!dupes.contains(val)) {
					sum += (Double) val;
					dupes.add(val);
				}
			}
			else {
				sum += (Double) val;
			}
		}
		return sum;
	}

	/**
	 * (Hwk 5 -- not given)
	 * Produces the average of the set of tuples for the given attribute position
	 * @param tuples	set of tuples
	 * @param agg_attr	name of the aggregating attribute
	 * @param distinct	whether to ignore duplicate values
	 * @return average of the attribute
	 */
	private Double agg_fn_avg(Collection<Tuple> tuples, String agg_attr, boolean distinct) {
		return agg_fn_sum(tuples,agg_attr,distinct)/tuples.size();
	}

	/**
	 * (Hwk 5 -- not given)
	 * Produces the max of the set of tuples for the given attribute position
	 * @param tuples	set of tuples
	 * @param agg_attr	name of the aggregating attribute
	 * @param distinct	whether to ignore duplicate values
	 * @return max value of the attribute
	 */
	private Comparable agg_fn_max(Collection<Tuple> tuples, String agg_attr, boolean distinct) {
		Set<Comparable> dupes = new HashSet<>();	// for distinct

		Comparable max = null;
		Comparable val;
		for (Tuple t : tuples) {
			val = t.valueOf(agg_attr);	// get the value to aggregate from the tuple

			if (distinct) {
				if (!dupes.contains(val)) {
					if (max == null || val.compareTo(max) > 0) {
						max = val;
					}
					dupes.add(val);
				}
			}
			else {
				if (max == null || val.compareTo(max) > 0) {
					max = val;
				}
			}
		}
		return max;
	}

	/**
	 * (Hwk 5 -- not given)
	 * Produces the min of the set of tuples for the given attribute position
	 * @param tuples	set of tuples
	 * @param agg_attr	name of the aggregating attribute
	 * @param distinct	whether to ignore duplicate values
	 * @return min value of the attribute
	 */
	private Comparable agg_fn_min(Collection<Tuple> tuples, String agg_attr, boolean distinct) {
		Set<Comparable> dupes = new HashSet<>();	// for distinct

		Comparable min = null;
		Comparable val;
		for (Tuple t : tuples) {
			val = t.valueOf(agg_attr);	// get the value to aggregate from the tuple

			if (distinct) {
				if (!dupes.contains(val)) {
					if (min == null || val.compareTo(min) < 0) {
						min = val;
					}
					dupes.add(val);
				}
			}
			else {
				if (min == null || val.compareTo(min) < 0) {
					min = val;
				}
			}
		}
		return min;
	}

	/**
	 * (Hwk 5 -- not given)
	 * This inner class defines a comparator to let us sort tuples
	 * into multiple groups. Sorts tuples by increasing group values.
	 */
	private static class GroupComparator implements Comparator<Tuple> {
		private String[] groups;

		public GroupComparator(Relation r, String[] groups) {
			this.groups = groups;
		}

		/**
		 * Compares two tuples to provide ordering
		 * @param t1
		 * @param t2
		 * @return -val if the former should be ordered before the latter, +val
		 * otherwise, or 0 if they are equal.
		 */
		@Override
		public int compare(Tuple t1, Tuple t2) {
			for (int i = 0; i < this.groups.length; i++) {
				Comparable first = t1.valueOf(this.groups[i]);
				Comparable second = t2.valueOf(this.groups[i]);
				if (first.compareTo(second) < 0) {
					return -1;
				}
				else if (first.compareTo(second) > 0) {
					return 1;
				}
			}
			return 0;
		}
	}






	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Relation hashJoin(Relation r1, Relation r2) throws DBException {

		//Precondition: The common attribute in R must be unique
		//Input: Relation r1, Relation r2
		//Output: Relation join

		// determine common attributes and use as key
		Set<Attribute> common = new HashSet<>(r1.getAttributes());
		common.retainAll(r2.getAttributes());
		if (common.size() == 0) {	// no common attributes, natural join reduces to product
			return times(r1,r2);
		}


		System.out.println("COMMON");
		for (Attribute a: common){
			System.out.println(a.getName());
		}


		// return this
		Relation join = new Relation();
		//set attribute list for join
		List<Attribute> newAtts = new ArrayList<Attribute>();







		// Phase I: Hash every tuple of R by the value
		// of the common attribute
		HashMap<List<Comparable>, List<Comparable>> map = new HashMap<List<Comparable>,List<Comparable>>();

		for (Tuple r : r1.tuples) {
			// grab values for attribute key c
			List c = new ArrayList<Comparable>();
			for (Attribute a : common){
				c.add(r.valueOf(a.getName()));
			}

			//check if no duplicate key exits add to the map
			if (!map.containsKey(c)) {
				//get values of tuple
				List<Comparable> newData = new ArrayList<Comparable>();
				newData = r.data; 
				//insert into map
				map.put(c, newData);
			}
			else {
				throw new DBException("Hash-join cannot be performed \nThe common attribute in R must be unique");
			}
		}

		// Phase II: Join up with r2
		for (Tuple r : r2.tuples) {
			// grab values for attribute key c
			List c = new ArrayList<Comparable>();
			for (Attribute a : common){
				c.add(r.valueOf(a.getName()));
			}




			//check if keyed value 
			if (map.containsKey(c)) {
				//get mapped value
				List<Comparable> data = new ArrayList<Comparable>();
				data.addAll(map.get(c));			

				//subtract common attribute out of r2
				List<Comparable> r2Data = new ArrayList<Comparable>();
				r2Data = r.data;
				r2Data.removeAll(c);

				//combine tuple values
				data.addAll(r2Data);


				System.out.println("DATA" + data.toString());

				//add tuple to relation
				Tuple tup = new Tuple(data,join);

				System.out.println(tup);
				System.out.println("SIZE: "+tup.size());



			}



		}


		return join;
	}

	@Override
	public double getElapsedTime() {
		return time;
	}

	@Override
	public void resetElapsedTime() {
		time = 0;	
	}


	private void resetTimers(){
		startTime=0;
		endTime=0;
	}
}