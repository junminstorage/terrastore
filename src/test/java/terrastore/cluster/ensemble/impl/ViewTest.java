/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.cluster.ensemble.impl;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import terrastore.communication.NodeConfiguration;

/**
 * @author Amir Moulavi
 */
public class ViewTest {

    private View viewB;
    private View viewA;
    private int percentage;

    @Test
    public void two_non_equal_sets_are_compared_correctly() {
        given_sets(
                construct_set("A", "B", "C"),
                construct_set("A", "C"));

        when_sets_are_compared();

        then_the_percentage_of_change_is(33);
    }

    @Test
    public void two_equal_size_different_members_sets_are_compared_correctly() {
        given_sets(
                construct_set("A", "B", "C"),
                construct_set("A", "C", "D"));

        when_sets_are_compared();

        then_the_percentage_of_change_is(50);
    }

    @Test
    public void two_equal_size_different_members_sets_are_compared_correctly_v2() {
        given_sets(
                construct_set("E", "B", "C"),
                construct_set("A", "F", "D"));

        when_sets_are_compared();

        then_the_percentage_of_change_is(100);
    }

    @Test
    public void two_equal_same_members_sets_are_compared_correctly() {
        given_sets(
                construct_set("A", "B", "C"),
                construct_set("A", "C", "B"));

        when_sets_are_compared();

        then_the_percentage_of_change_is(0);
    }

    @Test
    public void one_empty_set_with_one_non_emoty_set_are_compared_correctly() {
        given_sets(
                construct_set("A", "B", "C"),
                empty_set());

        when_sets_are_compared();

        then_the_percentage_of_change_is(100);
    }

    @Test
    public void two_empty_sets_are_compared_correctly() {
        given_sets(
                empty_set(),
                empty_set());

        when_sets_are_compared();

        then_the_percentage_of_change_is(0);
    }

    private void given_sets(View viewA, View viewB) {
        this.viewA = viewA;
        this.viewB = viewB;
    }

    private void when_sets_are_compared() {
        this.percentage = viewA.percentageOfChange(viewB);
    }

    private void then_the_percentage_of_change_is(int percentage) {
        Assert.assertTrue("Percentage doesn't match, expected [" + percentage + "], but it was [" + this.percentage + "]", this.percentage == percentage);
    }

    private View construct_set(String... membersName) {

        Set<View.Member> members = new HashSet<View.Member>();
        for (String member : membersName) {
            NodeConfiguration serverConfig = new NodeConfiguration(member, "127.0.0.1", 8080, "127.0.0.1", 8080);
            View.Member m = new View.Member(serverConfig);
            members.add(m);
        }
        View view = new View("cluster1", members);
        return view;
    }

    private View empty_set() {
        Set<View.Member> members = new HashSet<View.Member>();
        View view = new View("cluster1", members);
        return view;
    }

}
