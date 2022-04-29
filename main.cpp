//
// Created by ilana on 18/04/2022.
//

#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>
#include <iostream>

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};

class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};

class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;
};


class CounterClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const {
        std::array<int, 256> counts;
        counts.fill(0);
        for(const char& c : static_cast<const VString*>(value)->content) {
            counts[(unsigned char) c]++;
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar* k2 = new KChar(i);
            VCount* v2 = new VCount(counts[i]);
            usleep(150000);
            emit2(k2, v2, context);
        }
    }
    // (null,"hello") -> [(h,1),(e,1),(l,2),(o,1)]
    virtual void reduce(const IntermediateVec* pairs,
                        void* context) const {
        const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
        int count = 0;
        for(const IntermediatePair& pair: *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        KChar* k3 = new KChar(c);
        VCount* v3 = new VCount(count);
        usleep(150000);
        emit3(k3, v3, context);
    }
};

void printInputVector(const InputVec& inputVec){
    std::cout << "~~~~~~~~~~~ Input vector ~~~~~~~~~~~~"<<std::endl;

    int i = 0;
    for (auto &pair: inputVec) {
        auto str = dynamic_cast<const VString*>(pair.second)->content;
        std::cout << "Pair #"<< i <<": (nullptr, " << str << ")"<<std::endl;
        i++;
    }
    std::cout << "~~~~~~~~~~ ------------- ~~~~~~~~~~~~"<<std::endl;

}
int main(int argc, char** argv)
{
    std::cout << "The job: Counting character frequency in strings "<<std::endl;

    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    VString s1("Hello");
    VString s2("world");
    VString s3("ilana");
    VString s4("ido");
    VString s5("This string is full of characters");
    VString s6("Multithreading is awesome");
    VString s7("race conditions are bad");
    VString s8("LO");
    VString s9("A");
    VString s10("B");
    VString s11("C");
    VString s12("D");
    VString s13("E");
    VString s14("F");
    VString s15("G");
    VString s16("H");
    VString s17("P");
    VString s18("O");
    VString s19("R");
    VString s20("Q");
    VString s21("Stuff");
    VString s22("I");
    VString s23("Hate");
    VString s24("OS");
    VString s25("VERY");
    VString s26("MUCH");
    VString s27("SO");
    VString s28("MUCH");
    VString s29("YOU");
    VString s30("CANT");
    VString s31("IMAGINE");
    VString s32("MOTHERFUCKER");
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});
    inputVec.push_back({nullptr, &s4});
    inputVec.push_back({nullptr, &s5});
    inputVec.push_back({nullptr, &s6});
    inputVec.push_back({nullptr, &s7});
    inputVec.push_back({nullptr, &s8});
    inputVec.push_back({nullptr, &s9});
    inputVec.push_back({nullptr, &s10});
    inputVec.push_back({nullptr, &s11});
    inputVec.push_back({nullptr, &s12});
    inputVec.push_back({nullptr, &s13});
    inputVec.push_back({nullptr, &s14});
    inputVec.push_back({nullptr, &s15});
    inputVec.push_back({nullptr, &s16});
    inputVec.push_back({nullptr, &s17});
    inputVec.push_back({nullptr, &s18});
    inputVec.push_back({nullptr, &s19});
    inputVec.push_back({nullptr, &s20});
    inputVec.push_back({nullptr, &s21});
    inputVec.push_back({nullptr, &s22});
    inputVec.push_back({nullptr, &s23});
    inputVec.push_back({nullptr, &s24});
    inputVec.push_back({nullptr, &s25});
    inputVec.push_back({nullptr, &s26});
    inputVec.push_back({nullptr, &s27});
    inputVec.push_back({nullptr, &s28});
    inputVec.push_back({nullptr, &s29});
    inputVec.push_back({nullptr, &s30});
    inputVec.push_back({nullptr, &s31});
    inputVec.push_back({nullptr, &s32});


    printInputVector(inputVec);
    JobState state;
    JobState last_state={UNDEFINED_STAGE,0};
    JobHandle job = startMapReduceJob(client, inputVec, outputVec, 5);
    getJobState(job, &state);

	while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
	{

        if (last_state.stage != state.stage || last_state.percentage != state.percentage){
            printf("stage %d, %f%% \n",state.stage, state.percentage);
        }
//		usleep(2);
        last_state = state;
		getJobState(job, &state);
	}
	printf("stage %d, %f%% \n",state.stage, state.percentage);
	printf("Done!\n");

	closeJobHandle(job);

	for (OutputPair& pair: outputVec) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n",
			c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}

    return 0;
}
//

