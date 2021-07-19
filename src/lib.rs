use std::path::Path;

use djanco::database::*;
use djanco::log::*;

use djanco_ext::*;

use djanco::objects::*;


use chrono::{NaiveDateTime, Datelike};

use std::collections::BTreeMap;

use method_chains::MethodChaining;

use std::fs;
use std::io::Write;
use std::path::PathBuf;


#[djanco(May, 2021, subsets(Generic))]
pub fn my_query(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {

    // project_id,  year, chain_length, frequency 
    let mut path: PathBuf = PathBuf::from(output);
    path.push("chain_lengths");
    path.set_extension("csv");
        
    let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)?;
        
    writeln!(file, "project_id,year,chain_length,frequency")?;
    for project in database.projects() {
        let project_id = project.id();
        let last_commits = get_year_end_revision(project);

        for (year, commit) in last_commits {
            let chain_lengths = get_code_year_end_revision(commit);
            for (chain_length, freq) in chain_lengths {
                writeln!(file,"{},{},{},{}", project_id, year, chain_length, freq)?;
            }
            
        }
        
    }

    Ok(())
}

pub fn get_year_end_revision<'a>(project : ItemWithData<'a,Project> ) -> BTreeMap<i32, ItemWithData<'a,Commit> > {

    let mut commits_per_year = BTreeMap::<i32, Vec<ItemWithData<Commit>> >::new();
    let commits = project.commits_with_data().unwrap();

    for commit in commits {
        let time = NaiveDateTime::from_timestamp(commit.committer_timestamp().unwrap(), 0);

        let year = time.date().year();

        commits_per_year.entry(year).or_insert(Vec::new()).push(commit);
        
    }

    let mut last_commit_per_year =  BTreeMap::<i32, ItemWithData<Commit> >::new();
    for (year, commits) in commits_per_year {
        let last_commit = commits.into_iter().max_by_key(|commit| {
            commit.committer_timestamp().unwrap()
        });

        last_commit_per_year.insert(year, last_commit.unwrap());
    }
    last_commit_per_year    
}

pub fn get_code_year_end_revision<'a>(commit : ItemWithData<'a,Commit> ) -> BTreeMap<usize, usize>{
    
    let tree = commit.tree_with_data();

    let mut chain_lengths = Vec::<usize>::new();

    for change in tree.changes_with_data() {

        if let Some(snapshot) = change.snapshot() {
            // let file = change.path().unwrap().location();
            let contents = snapshot.contents();
            let chainings_counts = contents.method_chain_counts();

            chain_lengths.extend(chainings_counts.into_iter());
            

        }

    }

    chain_lengths
        .into_iter()
        .fold(
            BTreeMap::new(), 
            |mut accumulator, chain_length| {
                *accumulator.entry(chain_length).or_insert(0) += 1;
                accumulator
            }
        )
}


